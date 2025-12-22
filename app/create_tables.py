from app.db import reset_tables
from app.config import POSTGRES_URL


def main():
    reset_tables(POSTGRES_URL, echo=True)


if __name__ == "__main__":
    main()
