import requests


def get_data_employees(data: list) -> list[dict]:
    """
    Получает данные о компаниях (работодателях) по их идентификаторам.

    :param data: Список идентификаторов работодателей (employer_id).
    :type data: list[int]

    :return: Список словарей с данными о компаниях.
    :rtype: list[dict]
    """
    data_list = []
    for employer_id in data:
        url = f'https://api.hh.ru/employers/{employer_id}'
        response = requests.get(url)
        data_list.append({
            'employer_id': employer_id,
            'name_company': response.json()['name'],
            'vacancies_url': response.json()['vacancies_url']
        })
    return data_list


def get_vacancies_employer(data_id) -> list[dict]:
    """
    Получает данные о вакансиях, связанных с работодателями по их идентификаторам.

    :param data_id: Список идентификаторов работодателей (employer_id).
    :type data_id: list[int]

    :return: Список словарей с данными о вакансиях.
    :rtype: list[dict]
    """
    data_list = []
    for employer_id in data_id:
        params = {
            'per_page': 50
        }
        url = f'https://api.hh.ru/vacancies?employer_id={employer_id}'
        response = requests.get(url, params=params)
        items = response.json()['items']
        for item in items:
            data_list.append({
                'vacancy_id': item['id'],
                'vacancy_name': item['name'],
                'salary': item['salary']['from'] if item['salary'] and item['salary']['from'] is not None
                else 0,
                'vacancy_url': item['alternate_url'],
                'employer_id': item['employer']['id']
            })
    return data_list