from os import environ
from typing import Optional
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


def get_all_categories() -> list:
    conn = create_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM client_preferences;
                """
            )
            categories = [dict(row) for row in cursor.fetchall()]
            return list(categories) if categories else []
    except Exception as e:
        print(f"Unable to get all categories, {e}")
        conn.rollback()
    finally:
        conn.close()

    return []


def get_category(category: str) -> dict:
    conn = create_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT category, description, target FROM client_preferences WHERE LOWER(category) = LOWER(%s)
                """,
                (category,),
            )
            target = cursor.fetchone()
            return dict(target) if target else {}
    except Exception as e:
        print(f"Unable to get the category, {e}")
        conn.rollback()
    finally:
        conn.close()

    return {}


def create_category(category: str, description: str, target: str) -> dict:
    conn = create_connection()

    try:
        with conn.cursor() as cursor:
            # First check if the category already exists (case-insensitive)
            cursor.execute(
                """
                SELECT * FROM client_preferences 
                WHERE LOWER(category) = LOWER(%s);
                """,
                (category,),
            )
            existing_category = cursor.fetchone()

            if existing_category:
                # If category exists, return the existing record
                return dict(existing_category)

            # If category doesn't exist, create a new one
            cursor.execute(
                """
                INSERT INTO client_preferences (category, description, target)
                VALUES (%s, %s, %s) RETURNING *;
                """,
                (category, description, target),
            )
            conn.commit()
            new_category = cursor.fetchone()
            return dict(new_category) if new_category else {}
    except Exception as e:
        print(f"Unable to create a category, {e}")
        conn.rollback()
    finally:
        conn.close()

    return {}


def update_category(
    service_id: int,
    category: Optional[str] = None,
    description: Optional[str] = None,
    target: Optional[str] = None,
) -> dict:
    conn = create_connection()
    update_fields = []
    values = []

    # Build the update query dynamically based on provided arguments
    if category:
        update_fields.append("category = %s")
        values.append(category)
    if description:
        update_fields.append("description = %s")
        values.append(description)
    if target:  # Fixed the condition that was checking description instead of target
        update_fields.append("target = %s")
        values.append(target)

    if not update_fields:
        print("No fields provided to update.")
        return {}

    values.append(service_id)  # Add the service_id for the WHERE clause

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE client_preferences
                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE service_id = %s
                RETURNING *;
                """,
                tuple(values),
            )
            conn.commit()
            updated_category = cursor.fetchone()
            return dict(updated_category) if updated_category else {}
    except Exception as e:
        print(f"Unable to update category with service_id {service_id}, {e}")
        conn.rollback()
    finally:
        conn.close()

    return {}


def delete_category(service_id: int) -> bool:
    conn = create_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM client_preferences
                WHERE service_id = %s;
                """,
                (service_id,),
            )
            conn.commit()
            return cursor.rowcount > 0  # Return True if a row was deleted
    except Exception as e:
        print(f"Unable to delete category with service_id {service_id}, {e}")
        conn.rollback()
    finally:
        conn.close()

    return False


if __name__ == "__main__":
    pass
    # conn = create_connection()
    # with conn.cursor() as cursor:
    #     cursor.execute(
    #         f"""
    #             CREATE TABLE IF NOT EXISTS
    #             client_preferences (
    #                 service_id SERIAL PRIMARY KEY,
    #                 category TEXT NOT NULL,
    #                 description TEXT,
    #                 target TEXT,
    #                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    #                 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    #                 is_active BOOLEAN DEFAULT TRUE
    #             );
    #         """
    #     )
    #     conn.commit()

    # sales_category = create_category(
    #     category="sales",
    #     description="Sales is the process of persuading potential customers to purchase a product or service. It involves identifying customer needs, building relationships, presenting solutions, and facilitating transactions that provide value to both the buyer and seller. Sales professionals use various techniques to understand customer preferences, overcome objections, and close deals. Sales is fundamental in driving revenue, fostering customer loyalty, and supporting business growth across industries.",
    #     target="https://docs.google.com/presentation/d/19pjmei97khTvtdYgtI4Kkw7F21SdMLQLeSRKF36b29g/edit?usp=sharing",
    # )

    # ai_category = create_category(
    #     category="ai",
    #     description="Sales is the process of persuading potential customers to purchase a product or service. It involves identifying customer needs, building relationships, presenting solutions, and facilitating transactions that provide value to both the buyer and seller. Sales professionals use various techniques to understand customer preferences, overcome objections, and close deals. Sales is fundamental in driving revenue, fostering customer loyalty, and supporting business growth across industries.",
    #     target="https://docs.google.com/presentation/d/19pjmei97khTvtdYgtI4Kkw7F21SdMLQLeSRKF36b29g/edit?usp=sharing",
    # )