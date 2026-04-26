import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

# Constantes
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'
FIXED_CLASSIFICATION_ID = 13
URL_BASE = "https://www.ani.gov.co/informacion-de-la-ani/normatividad?field_tipos_de_normas__tid=12&title=&body_value=&field_fecha__value%5Bvalue%5D%5Byear%5D="

CLASSIFICATION_KEYWORDS = {
    'resolución': 15,
    'resolucion': 15,
    'decreto': 14,
}
DEFAULT_RTYPE_ID = 14


def clean_quotes(text):
    if not text:
        return text
    quotes_map = {
        '\u201C': '', '\u2018': '', '\u2019': '', '\u00AB': '', '\u00BB': '',
        '\u201E': '', '\u201A': '', '\u2039': '', '\u203A': '', '"': '',
        "'": '', '´': '', '`': '', '′': '', '″': '',
    }
    cleaned_text = text
    for quote_char, replacement in quotes_map.items():
        cleaned_text = cleaned_text.replace(quote_char, replacement)
    quotes_pattern = r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033]'
    cleaned_text = re.sub(quotes_pattern, '', cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text


def get_rtype_id(title):
    title_lower = title.lower()
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    return DEFAULT_RTYPE_ID


def is_valid_created_at(created_at_value):
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False


def extract_title_and_link(row, norma_data, row_num):
    title_cell = row.find('td', class_='views-field views-field-title')
    if not title_cell:
        logger.debug(f"No se encontró celda de título en fila {row_num}. Saltando.")
        return False

    title_link = title_cell.find('a')
    if not title_link:
        logger.debug(f"No se encontró enlace en fila {row_num}. Saltando.")
        return False

    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)

    if len(cleaned_title) > 65:
        logger.debug(f"Título demasiado largo en fila {row_num}: '{cleaned_title}' ({len(cleaned_title)} chars). Saltando.")
        return False

    norma_data['title'] = cleaned_title

    external_link = title_link.get('href')
    if external_link and not external_link.startswith('http'):
        external_link = 'https://www.ani.gov.co' + external_link

    norma_data['external_link'] = external_link
    norma_data['gtype'] = 'link' if external_link else None

    if not norma_data['external_link']:
        logger.debug(f"Sin enlace externo válido en fila {row_num}. Saltando.")
        return False

    return True


def extract_summary(row, norma_data):
    summary_cell = row.find('td', class_='views-field views-field-body')
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        norma_data['summary'] = cleaned_summary.capitalize()
    else:
        norma_data['summary'] = None


def extract_creation_date(row, norma_data, row_num):
    fecha_cell = row.find('td', class_='views-field views-field-field-fecha--1')
    if fecha_cell:
        fecha_span = fecha_cell.find('span', class_='date-display-single')
        if fecha_span:
            created_at_raw = fecha_span.get('content', fecha_span.get_text(strip=True))
            if 'T' in created_at_raw:
                norma_data['created_at'] = created_at_raw.split('T')[0]
            elif '/' in created_at_raw:
                try:
                    day, month, year = created_at_raw.split('/')
                    norma_data['created_at'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception:
                    norma_data['created_at'] = created_at_raw
            else:
                norma_data['created_at'] = created_at_raw
        else:
            norma_data['created_at'] = fecha_cell.get_text(strip=True)
    else:
        norma_data['created_at'] = None

    if not is_valid_created_at(norma_data['created_at']):
        logger.debug(f"Fecha inválida en fila {row_num}: '{norma_data['created_at']}'. Saltando.")
        return False

    return True


def scrape_page(page_num):
    if page_num == 0:
        page_url = URL_BASE
    else:
        page_url = f"{URL_BASE}&page={page_num}"

    logger.info(f"Scrapeando página {page_num}: {page_url}")

    try:
        response = requests.get(page_url, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        tbody = soup.find('tbody')

        if not tbody:
            logger.warning(f"No se encontró tabla en página {page_num}")
            return []

        rows = tbody.find_all('tr')
        logger.info(f"Encontradas {len(rows)} filas en página {page_num}")

        page_data = []
        for i, row in enumerate(rows, 1):
            try:
                norma_data = {
                    'created_at': None,
                    'update_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'is_active': True,
                    'title': None,
                    'gtype': None,
                    'entity': ENTITY_VALUE,
                    'external_link': None,
                    'rtype_id': None,
                    'summary': None,
                    'classification_id': FIXED_CLASSIFICATION_ID,
                }

                if not extract_title_and_link(row, norma_data, i):
                    continue

                extract_summary(row, norma_data)

                if not extract_creation_date(row, norma_data, i):
                    continue

                norma_data['rtype_id'] = get_rtype_id(norma_data['title'])
                page_data.append(norma_data)

            except Exception as e:
                logger.warning(f"Error procesando fila {i} en página {page_num}: {e}")
                continue

        return page_data

    except requests.RequestException as e:
        logger.error(f"Error HTTP en página {page_num}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error procesando página {page_num}: {e}")
        return []


def run_extraction(num_pages=9):
    """
    Punto de entrada del módulo de extracción.
    Retorna lista de registros extraídos.
    """
    logger.info(f"Iniciando extracción — páginas a procesar: {num_pages}")
    all_data = []

    for page_num in range(num_pages):
        page_data = scrape_page(page_num)
        all_data.extend(page_data)

        if (page_num + 1) % 3 == 0:
            logger.info(f"Progreso: {page_num + 1}/{num_pages} páginas. Registros acumulados: {len(all_data)}")

    logger.info(f"Extracción completa — total registros extraídos: {len(all_data)}")
    return all_data