import psycopg2
from psycopg2 import Error


class DBManager:
    """
    Конструктор класса DBManager, инициализирующий соединение с базой данных.
    :param database: Название базы данных.
    :param host: Адрес хоста базы данных.
    :param user: Имя пользователя для доступа к базе данных.
    :param password: Пароль пользователя для доступа к базе данных.
    :param port: Порт, используемый для соединения с базой данных.
    """

    def __init__(self, database, host, user, password, port):
        self.connect = psycopg2.connect(database=database, host=host, user=user, password=password, port=port)

    def create_tables(self, name_table: str, data_file: str) -> None:
        """
        Создает таблицу в базе данных.
        :param name_table: Название таблицы.
        :param data_file: Описание структуры таблицы.
        """
        with self.connect:
            with self.connect.cursor() as cur:
                try:
                    cur.execute(f'CREATE TABLE {name_table} ({data_file})')
                except Error as e:
                    print(f'Таблица "{name_table}" уже существует : {e}')

    def clear_table(self, name_table: str) -> None:
        """
        Очищает данные из указанной таблицы.
        :param name_table: Название таблицы.
        """
        with self.connect:
            with self.connect.cursor() as cur:
                cur.execute(f"DELETE FROM {name_table} WHERE 1=1")

    def insert_data_into_db(self, name_table: str, insert_data) -> None:
        """
        Вставляет данные в указанную таблицу.
        :param name_table: Название таблицы.
        :param insert_data: Данные для вставки.
        """
        with self.connect:
            with self.connect.cursor() as cur:
                cur.execute(f'INSERT INTO {name_table} VALUES {insert_data}')

    def get_companies_and_vacancies_count(self) -> list[tuple]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        """
        with self.connect:
            with self.connect.cursor() as cur:
                cur.execute('''SELECT company_name, COUNT(vacancy_name) AS count_vacancies FROM employees 
                INNER JOIN vacancies USING(employer_id)
                GROUP BY company_name''')
                return cur.fetchall()

    def get_all_vacancies(self) -> list[tuple]:
        """
        получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию.
        :return:
        """
        with self.connect:
            with self.connect.cursor() as cur:
                cur.execute('SELECT company_name, vacancy_name, salary, vacancies_url  FROM employees INNER JOIN '
                            'vacancies USING (employer_id)')
                return cur.fetchall()

    def get_avg_salary(self) -> list[tuple]:
        """
        получает среднюю зарплату по вакансиям.
        :return:
        """
        with self.connect:
            with self.connect.cursor() as cur:
                cur.execute('SELECT ROUND(AVG(salary)::numeric) FROM vacancies')
                return cur.fetchall()

    def get_vacancies_with_higher_salary(self) -> list[tuple]:
        """
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        :return:
        """
        with self.connect:
            with self.connect.cursor() as cur:
                cur.execute('''SELECT vacancy_name, salary FROM VACANCIES
                                GROUP BY VACANCY_NAME, salary 
                                HAVING AVG(salary) > (SELECT AVG(salary) FROM VACANCIES)
                                ORDER BY salary DESC''')
                return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> list[tuple]:
        """
        получает список всех вакансий, в названии которых содержатся переданные в метод слова,
        например python.
        :return:
        """
        with self.connect:
            with self.connect.cursor() as cur:
                try:
                    cur.execute(f"SELECT vacancy_name FROM vacancies  WHERE vacancy_name  ILIKE '%{keyword}%'")
                except Error as e:
                    print(f'Ошибка в получении запроса: {e}')
                return cur.fetchall()