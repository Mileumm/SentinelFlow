Passer en venv :
.venv\Scripts\Activate.ps1

Passer du venv au requierments.txt :
python -m pip freeze > requirements.txt

Installer l'env par le requierments.txt :
pip install --no-cache-dir -r requirements.txt