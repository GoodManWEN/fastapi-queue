
from setuptools import setup, find_packages
from requests import get as rget
from bs4 import BeautifulSoup
import logging , sys
# init logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler(stream=sys.stdout) 
format = logging.Formatter("%(message)s")#("%(asctime)s - %(message)s") 
sh.setFormatter(format)
logger.addHandler(sh)

#
def get_install_requires(filename):
    with open(filename,'r') as f:
        lines = f.readlines()
    return [x.strip() for x in lines]

# 
url = 'https://github.com/GoodManWEN/fastapi-queue'
release = f'{url}/releases/latest'
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8"
}

html = BeautifulSoup(rget(url , headers).text ,'lxml')
description = html.find('meta' ,{'name':'description'}).get('content')
for kw in (' - GitHub', ' - GoodManWEN'):
    if ' - GitHub' in description:
        description = description[:description.index(' - GitHub')]
html = BeautifulSoup(rget(release , headers).text ,'lxml')
logger.info(f"description: {description}")

#
with open('tagname','r',encoding='utf-8') as f:
    version = f.read()
if ':' in version:
    version = version[:version.index(':')].strip()
version = version.strip()
logger.info(f"version: {version}")

#
with open('README.md','r',encoding='utf-8') as f:
    long_description_lines = f.readlines()

long_description_lines_copy = long_description_lines[:]
long_description_lines_copy.insert(0,'r"""\n')
long_description_lines_copy.append('"""\n')

# update __init__ docs
with open('fastapi_queue/__init__.py','r',encoding='utf-8') as f:
    init_content = f.readlines()

for line in init_content:
    if line == "__version__ = ''\n":
        long_description_lines_copy.append(f"__version__ = '{version}'\n")
    else:
        long_description_lines_copy.append(line)

with open('fastapi_queue/__init__.py','w',encoding='utf-8') as f:
    f.writelines(long_description_lines_copy)

setup(
    name="fastapi-queue", 
    version=version,
    author="WEN",
    description=description,
    long_description=''.join(long_description_lines),
    long_description_content_type="text/markdown",
    url="https://github.com/GoodManWEN/fastapi-queue",
    packages = find_packages(),
    install_requires = get_install_requires('requirements.txt'),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Framework :: AsyncIO',
    ],
    python_requires='>=3.7',
    keywords=["fastapi-queue" ,]
)
