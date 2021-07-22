# Тестовое задание _Improvado_

Для запуска:
1. Клонировать репозиторий и перейти в его папку
    ```
    git clone https://github.com/MarkerViktor/ImprovadoTestTask.git
    cd ImprovadoTestTask
    ```
2. Выполнить соответствующий скрипт (`basic.py` или `advanced.py`) при помощи `Python 3.9`, задав позиционными аргументами 1-м – название файла-результата, 2-м – дирректорию, содержащую файлы с данными.
    ```
    python basic.py ./basic_results.tsv ./data
    python advanced.py ./advance_results.tsv ./data
    ```
  
### Поддержка файлов иных форматов
Ипользуемая для парсинга функция определяется по расширению файла. Для реализации поддержки обработки файлов иных форматов в директории `solution/parsers` необходимо создать модуль, содержащий функцию-парсер, обладающую следующей сигнатурой:
```
parse_func(file_like_object: IO) -> (schema: dict[str, type]), rows_iterator: Iterable[tuple[Any]])
```
где `schema` – словарь заголовков-типов строк,
    `rows_itearator` – итератор кортежей строк,
    `file_like_object` – объект, предоставляющий API файлов.

А также импортировать модуль в файл `solution/parsers/__init__.py` и добавить функцию-парсер в словарь `parsers` с ключём – расширением файлов нового формата.
При возникновении ошибок в процессе парсинга функция должна выбрасывать исключение ParseError из модуля `solution/parser/utils.py`. В даном случае исключение будет обработано, а вызвавший ошибку файл пропущен.
