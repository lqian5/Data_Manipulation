import time
import numpy as np
import logging
import asyncio
from async_timeout import timeout
import ssl, certifi
from aiohttp import ClientSession, ClientResponseError, TCPConnector, ClientConnectorError


def sleep_between_retry(url):
    x = 2
    logging.warning("URL {} is triggering the a retry, sleeping for {} seconds...".format(url, x))
    time.sleep(x)


async def fetch(row, session):

    HTTP_STATUS_CODES_TO_RETRY = [500, 502, 503, 504]
    num_retries = 2
    url = row['url']

    for x in range(num_retries):
        error_message = ''
        try:
            # async with timeout(5):
            async with session.get(url, timeout=20, proxy='http://sysproxy.wal-mart.com:8080') as response:
                resp = await response.read()
                im_array = np.asarray(bytearray(resp), dtype=np.uint8)
                return row, im_array

        except ClientResponseError as e:
            error_message = 'Image download failed for {}. Received bad status {}'.format(url, e)
            logging.warning(error_message)
            if e.code == 404:
                logging.warning(row)
                return row, None
            # retry if it failed with connection issue
            elif e.code in HTTP_STATUS_CODES_TO_RETRY:
                sleep_between_retry(x, url)
                continue
            else:
                break

        # retry if it failed because of timeout
        except asyncio.TimeoutError:
            error_message = 'Image download failed for {}, timeout error'.format(url)
            logging.warning('%s Timeout', url)
            sleep_between_retry(url)
            continue
        except Exception as e:
            error_message = 'Image download failed for {}, Unexpected error {}'.format(url, e)
            logging.warning('Expected error for {}. {}'.format(url, e))
            break

    return row, None


async def fetch_all(input_dict, is_BytesIO=True):
    """Launch requests for all web pages."""
    tasks = []
    image_array = list()
    file_list = list()
    async with ClientSession(raise_for_status=True, connector=TCPConnector(verify_ssl=False)) as session:
        for row in input_dict:
            task = asyncio.ensure_future(fetch(row, session)) # row, session
            tasks.append(task) # create list of tasks
        for response in await asyncio.gather(*tasks): # gather task responses
            image_array.append(response[1])
            file_list.append(response[0])
    logging.warning('the {} batch succeeded'.format(len(input_dict)))
    return file_list, image_array


