"""
Dagster Assets for ETL Pipeline
Extract data from API, transform, and load to CSV/Database
"""
from dagster import asset, AssetExecutionContext
import pandas as pd
import requests
from datetime import datetime
import sqlite3
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



@asset(group_name="extract")
def raw_user_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract user data from JSONPlaceholder API"""
    context.log.info("Fetching user data from API...")
    
    # response = requests.get("https://jsonplaceholder.typicode.com/users")
    response = requests.get("https://jsonplaceholder.typicode.com/users", verify=False, timeout=30)

    response.raise_for_status()
    
    users = response.json()
    df = pd.DataFrame(users)
    
    context.log.info(f"Extracted {len(df)} users")
    return df


@asset(group_name="extract")
def raw_post_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract post data from JSONPlaceholder API"""
    context.log.info("Fetching post data from API...")
    
    # response = requests.get("https://jsonplaceholder.typicode.com/posts")
    response = requests.get("https://jsonplaceholder.typicode.com/posts", verify=False, timeout=30)
    response.raise_for_status()
    
    posts = response.json()
    df = pd.DataFrame(posts)
    
    context.log.info(f"Extracted {len(df)} posts")
    return df


@asset(group_name="transform")
def cleaned_user_data(context: AssetExecutionContext, raw_user_data: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform user data"""
    context.log.info("Transforming user data...")
    
    df = raw_user_data.copy()
    
    # Flatten nested columns
    df['city'] = df['address'].apply(lambda x: x.get('city', ''))
    df['company_name'] = df['company'].apply(lambda x: x.get('name', ''))
    
    # Select relevant columns
    df = df[['id', 'name', 'username', 'email', 'phone', 'city', 'company_name']]
    
    # Clean email
    df['email'] = df['email'].str.lower()
    
    context.log.info(f"Cleaned {len(df)} user records")
    return df


@asset(group_name="transform")
def enriched_post_data(context: AssetExecutionContext, raw_post_data: pd.DataFrame, raw_user_data: pd.DataFrame) -> pd.DataFrame:
    """Enrich post data with user information"""
    context.log.info("Enriching post data...")
    
    posts = raw_post_data.copy()
    users = raw_user_data[['id', 'name', 'username']].copy()
    users.columns = ['userId', 'author_name', 'author_username']
    
    # Join posts with users
    enriched = posts.merge(users, on='userId', how='left')
    
    # Add metadata
    enriched['word_count'] = enriched['body'].str.split().str.len()
    enriched['processed_at'] = datetime.now().isoformat()
    
    context.log.info(f"Enriched {len(enriched)} posts")
    return enriched


@asset(group_name="load")
def user_data_csv(context: AssetExecutionContext, cleaned_user_data: pd.DataFrame):
    """Load cleaned user data to CSV"""
    output_path = "data/users_cleaned.csv"
    cleaned_user_data.to_csv(output_path, index=False)
    context.log.info(f"Saved user data to {output_path}")


@asset(group_name="load")
def post_data_csv(context: AssetExecutionContext, enriched_post_data: pd.DataFrame):
    """Load enriched post data to CSV"""
    output_path = "data/posts_enriched.csv"
    enriched_post_data.to_csv(output_path, index=False)
    context.log.info(f"Saved post data to {output_path}")


@asset(group_name="load")
def analytics_database(context: AssetExecutionContext, cleaned_user_data: pd.DataFrame, enriched_post_data: pd.DataFrame):
    """Load data to SQLite database"""
    db_path = "data/analytics.db"
    
    with sqlite3.connect(db_path) as conn:
        cleaned_user_data.to_sql('users', conn, if_exists='replace', index=False)
        enriched_post_data.to_sql('posts', conn, if_exists='replace', index=False)
    
    context.log.info(f"Loaded data to {db_path}")


@asset(group_name="analytics", deps=["analytics_database"])
def user_post_summary(context: AssetExecutionContext) -> pd.DataFrame:
    """Generate summary statistics"""
    db_path = "data/analytics.db"
    
    query = """
    SELECT 
        u.name,
        u.city,
        u.company_name,
        COUNT(p.id) as total_posts,
        AVG(p.word_count) as avg_word_count
    FROM users u
    LEFT JOIN posts p ON u.id = p.userId
    GROUP BY u.id, u.name, u.city, u.company_name
    ORDER BY total_posts DESC
    """
    
    with sqlite3.connect(db_path) as conn:
        summary = pd.read_sql_query(query, conn)
    
    summary.to_csv("data/user_post_summary.csv", index=False)
    context.log.info(f"Generated summary for {len(summary)} users")
    
    return summary