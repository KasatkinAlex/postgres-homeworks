import csv
import psycopg2


def read_csv(file_csv) -> list:
    """Чтение файла с расширением .csv return список данных"""
    data_list = []
    with open(file_csv, "r", encoding="UTF-8") as file:
        data_csv = csv.reader(file)
        next(data_csv)
        for i in data_csv:
            data_list.append(i)
        return data_list


conn_params = {
    "host": "localhost",
    "database": "north",
    "user": "postgres",
    "password": "Otislift1990"
}


def main():
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:

            customers = read_csv("north_data/customers_data.csv")
            for customer in customers:
                cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (customer[0], customer[1], customer[2]))

            employees = read_csv("north_data/employees_data.csv")
            for employee in employees:
                cur.execute(
                    "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                    (employee[0], employee[1], employee[2], employee[3], employee[4], employee[5])
                )

            orders = read_csv("north_data/orders_data.csv")
            for order in orders:
                cur.execute(
                    "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                    (order[0], order[1], order[2], order[3], order[4])
                )
    conn.close()


if __name__ == "__main__":
    main()
