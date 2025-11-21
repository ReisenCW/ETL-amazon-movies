"""Utility to wipe data tables in amazon_movies.db while keeping the schema."""

import sqlite3
from pathlib import Path

TABLES_IN_DELETE_ORDER = [
	"final_movie_reviews",
	"data_lineage",
	"name_normalization",
	"final_movies",
	"raw_reviews",
	"raw_products",
]


def clear_db(db_path: Path | None = None) -> None:
	"""Remove all rows from the known ETL tables."""

	project_root = Path(__file__).resolve().parent.parent
	if db_path is None:
		db_path = project_root / "database" / "amazon_movies.db"

	if not db_path.exists():
		raise FileNotFoundError(f"Database not found at {db_path}")

	conn = sqlite3.connect(db_path)
	try:
		cursor = conn.cursor()
		cursor.execute("PRAGMA foreign_keys = OFF")

		for table in TABLES_IN_DELETE_ORDER:
			cursor.execute(
				"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
				(table,),
			)
			if cursor.fetchone() is None:
				continue

			cursor.execute(f"SELECT COUNT(*) FROM {table}")
			count = cursor.fetchone()[0]
			cursor.execute(f"DELETE FROM {table}")
			print(f"Cleared {count} rows from {table}")

		conn.commit()
		cursor.execute("VACUUM")
		print("Database cleared successfully.")
	finally:
		conn.close()


def main() -> None:
	try:
		clear_db()
	except FileNotFoundError as exc:
		print(exc)


if __name__ == "__main__":
	main()
