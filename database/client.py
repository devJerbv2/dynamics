import json

from os import environ
from typing import Optional, Tuple
from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, find_dotenv

# Load and retrieve all environment variables
_ = load_dotenv(find_dotenv())

# Database credentials
DB_USERNAME = environ["DB_USERNAME"]
DB_PASSWORD = environ["DB_PASSWORD"]
DB_SERVER = environ["DB_SERVER"]
DB_HOST = environ["DB_HOST"]
DB_PORT = environ["DB_PORT"]
DB_NAME = environ["DB_NAME"]

DB_URI = f"postgres://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}.{DB_SERVER}/{DB_NAME}"


def create_connection(db_uri: str = DB_URI) -> Optional[connection | None]:
    try:
        return connect(db_uri, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"Unable to create a connection, {e}")
    return None


def get_all_client_info() -> list:
    conn = create_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM client_info;
                """
            )
            categories = [dict(row) for row in cursor.fetchall()]
            return list(categories) if categories else []
    except Exception as e:
        print(f"Unable to get all client info, {e}")
        conn.rollback()
    finally:
        conn.close()

    return []


def get_client_info(client_name: str) -> dict:
    """Get client information by name."""
    conn = create_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM client_info WHERE client_name = %s;
                """,
                (client_name.lower(),),  # Convert to lowercase for consistency
            )
            client_info = cursor.fetchone()
            return dict(client_info) if client_info else {}

    except Exception as e:
        print(f"Unable to get client info, {e}")
        conn.rollback()
    finally:
        conn.close()
    return {}


def get_or_create_client(cursor, client_name: str) -> Tuple[int, bool]:
    """
    Get existing client or create a new one if doesn't exist.
    Returns tuple of (client_id, was_created)
    """
    # First try to get existing client
    cursor.execute(
        """
        SELECT client_id FROM client_info WHERE client_name = %s;
        """,
        (client_name.lower(),),  # Convert to lowercase for consistency
    )
    result = cursor.fetchone()

    if result:
        return result["client_id"], False

    # If client doesn't exist, create new one
    cursor.execute(
        """
        INSERT INTO client_info (client_name, preferences)
        VALUES (%s, %s)
        RETURNING client_id;
        """,
        (client_name.lower(), json.dumps({})),
    )
    new_client = cursor.fetchone()
    return new_client["client_id"], True


def update_client_info(client_name: str, category: str) -> bool:
    """
    Update client preferences, creating the client if they don't exist.
    Returns True if operation was successful, False otherwise.
    """
    conn = create_connection()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            # Get or create client
            client_id, was_created = get_or_create_client(cursor, client_name)

            if was_created:
                # If this is a new client, initialize with category count of 1
                cursor.execute(
                    """
                    UPDATE client_info
                    SET preferences = jsonb_set(preferences, %s, %s::jsonb)
                    WHERE client_id = %s
                    """,
                    (f'{{"{category}"}}', json.dumps(1), client_id),
                )
            else:
                # For existing client, increment the category count
                cursor.execute(
                    """
                    WITH current_value AS (
                        SELECT COALESCE(
                            (preferences->%s)::integer,
                            0
                        ) as value
                        FROM client_info
                        WHERE client_id = %s
                    )
                    UPDATE client_info
                    SET preferences = jsonb_set(
                        COALESCE(preferences, '{}'::jsonb),
                        %s,
                        (to_jsonb((SELECT value + 1 FROM current_value)))
                    )
                    WHERE client_id = %s
                    """,
                    (category, client_id, f'{{"{category}"}}', client_id),
                )

            conn.commit()
            return True

    except Exception as e:
        print(f"Unable to update client info, {e}")
        conn.rollback()
    finally:
        conn.close()
    return False


if __name__ == "__main__":
    conn = create_connection()

    # Test creating a new client with update
    result = update_client_info("test user", "sales")
    print(f"Update result: {result}")

    # Get and print the client info
    client_info = get_client_info("test user")
    print(f"Client info: {client_info}")

    # Update again to test increment
    result = update_client_info("test user", "ai")
    print(f"Second update result: {result}")

    # Get and print updated info
    client_info = get_client_info("test user")
    print(f"Updated client info: {client_info}")

    # with conn.cursor() as cursor:
    #     cursor.execute(
    #         """
    #             CREATE TABLE IF NOT EXISTS client_info (
    #             client_id SERIAL PRIMARY KEY,
    #             client_name TEXT NOT NULL,
    #             preferences JSONB,
    #             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    #             updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    #             is_active BOOLEAN DEFAULT TRUE
    #         );
    #     """
    #     )
    #     conn.commit()
    #     conn.close()

    # with conn.cursor() as cursor:
    #     cursor.execute("""TRUNCATE TABLE client_info""")
    #     conn.commit()

    # with conn.cursor() as cursor:
    #     cursor.execute("""DROP TABLE client_info""")
    #     conn.commit()
