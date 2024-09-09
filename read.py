import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import os

# Load environment variables
load_dotenv()

# Connection settings
HOST = os.getenv('HOST', 'localhost')
USER = os.getenv('USER','root')
PASSWORD = os.getenv('PASSWORD','11032005m')
DATABASE = os.getenv('DATABASE','test_db')


def create_connection():
    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None


def read_uncommited():
    """
    Shows how READ UNCOMMITED isolation level works.
    Shows dirty read.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Read Uncommitted
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ UNCOMMITTED')
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")

        # Transaction 2: Read Uncommitted
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ UNCOMMITTED')
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_dirty_read = cursor2.fetchone()[0]

        print(f"Dirty Read (READ UNCOMMITTED): Alice's balance = {balance_dirty_read}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def read_commited():
    """
    Shows how READ COMMITED isolation level works.
    Shows NO dirty read.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Read Uncommitted
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ UNCOMMITTED')
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")

        # Transaction 2: Read Committed
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_committed_read = cursor2.fetchone()[0]

        print(f"Read (READ COMMITTED): Alice's balance = {balance_committed_read}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def repeatable_read():
    """
    Demonstrates how REPEATABLE READ isolation level works.
    Ensures that the same data is read multiple times with the same result.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()
    connection3 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()
        cursor3 = connection3.cursor()

        # Transaction 1: Read Uncommitted
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='REPEATABLE READ')
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_before = cursor1.fetchone()[0]
        print(f"Transaction 1: Alice's initial balance = {balance_before}")

        # Transaction 2: Update balance
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")
        connection2.commit()

        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_after = cursor1.fetchone()[0]
        print(f"Transaction 1: Alice's balance after update = {balance_after}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        # Transaction 3: Set initial balance to 1000
        connection3.start_transaction(isolation_level='READ COMMITTED')
        cursor3.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
        connection3.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()
        if cursor3:
            cursor3.close()
        if connection3 and connection3.is_connected():
            connection3.close()

def non_repeatable_read():
    """
    Demonstrates non-repeatable read with READ COMMITTED isolation level.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()
    connection3 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()
        cursor3 = connection3.cursor()

        # Transaction 1: Read balance (first time)
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ COMMITTED')
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_before = cursor1.fetchone()[0]
        print(f"Transaction 1: Alice's initial balance = {balance_before}")

        # Transaction 2: Update balance
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")
        print(f"Transaction 2 updated Alice's balance to 9999")
        connection2.commit()

        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_after = cursor1.fetchone()[0]
        print(f"Transaction 1: Alice's balance after update in another transaction = {balance_after}")

        print(f"Transaction 1 commit(): {datetime.now()}")
        connection1.commit()

        # Transaction 3: Set initial balance to 1000
        connection3.start_transaction(isolation_level='READ COMMITTED')
        cursor3.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
        connection3.commit()


    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()
        if cursor3:
            cursor3.close()
        if connection3 and connection3.is_connected():
            connection3.close()


def deadlock():
    """
    Demonstrates a deadlock scenario
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Locks Alice's account
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction()
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")
        print("Transaction 1 locked Alice's account.")

        # Transaction 2: Locks Bob's account
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction()
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Bob' FOR UPDATE")
        print("Transaction 2 locked Bob's account.")

        print("Transaction 1 attempts to lock Bob's account (Transction 2 is blocking)...")
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Bob' FOR UPDATE")

        print("Transaction 2 attempts to lock Alice's account (blocked by Transaction 1)...")
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")

        connection1.commit()
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")

    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

if __name__ == "__main__":
    print("Demonstrating Dirty Read with READ UNCOMMITTED")
    read_uncommited()

    print("\nDemonstrating No Dirty Read with READ COMMITTED")
    read_commited()

    print("\nDemonstrating REPEATBLE READ")
    repeatable_read()

    print("\nDemonstrating NON-REPEATBLE READ")
    non_repeatable_read()

    print("\nDemonstrating Deadlock")
    deadlock()