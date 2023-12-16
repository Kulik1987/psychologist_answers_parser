import argparse
import bs4
from datetime import datetime
from functools import reduce
import time
from multiprocessing import Pool
import pandas as pd
from pathlib import Path
import requests
from tqdm import tqdm

from const import n_workers, n_waiting_seconds, max_url_attempts_number


def get_soup(url):
    '''get html code from page with Anti-DDos bypass'''
    url_attempts_number = 0
    while True:
        try:
            req = requests.get(url)
            return bs4.BeautifulSoup(req.text, 'lxml')
        except Exception as exception:
            print(f'We catch {exception} with url {url}, we will wait {n_waiting_seconds} seconds against Anti-DDos protection')   
            time.sleep(n_waiting_seconds)
            url_attempts_number += 1
            if url_attempts_number == max_url_attempts_number:
                return bs4.BeautifulSoup('', 'lxml')


def get_info_from_page(url):
    '''get question and answers in url'''
    soup = get_soup(url)

    if soup.text == '':
        return [{'url': url}]
    else:
        question_title = soup.find('div', 'vopros').find('div', 'zag').text.strip()
        question_body = soup.find('div', 'vopros').find('div', 'text').text
        answers = [answer.text for answer in soup.findAll('div', 'otvet')]

    return [{'url': url, 
             'question_title': question_title, 
             'question_body': question_body, 
             'answers': answers}]


def get_pages_range():
    '''get pages range'''
    main_url = 'https://psiholog.ru/otvety-psihologov'
    soup = get_soup(main_url)
    last_question_url = soup.find('div', 'vopros').find('div', 'zag').find('a', href=True)['href']
    last_question_id = int(last_question_url.split('/')[-1])
    question_ids = range(1, last_question_id + 1)
    pages_range = [f'https://psiholog.ru/vopros/{i}' for i in question_ids]
    return pages_range


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Parser of questions from psiholog.ru site')
    parser.add_argument(
        '--save_path',
        help='path for saving data',
        type=Path,
        required=True
    )
    args = parser.parse_args()
    print(f'Run with arguments: {args}')

    print("Let's find all question urls")
    questions_url_list = get_pages_range()

    print("Let's parse all question urls")
    with Pool(n_workers) as p:
        maped_questions = tqdm(p.imap_unordered(get_info_from_page, questions_url_list), total=len(questions_url_list))
        questions_data = pd.DataFrame(reduce(lambda x, y: x + y, maped_questions))

    current_datetime = datetime.today().strftime('%Y_%m_%d')
    questions_data.to_csv(args.save_path / f'psiholog_{current_datetime}.csv', index=False)

    print("Well done")