import requests
import json


# URL για το One Call API του OpenWeatherMap
url = f"https://api.openweathermap.org/data/2.5/weather?lat=25.276987&lon=55.296249&appid=26d2a4e587fc68ba1d98399638a19231"

# Κάνοντας το αίτημα για τα δεδομένα καιρού
response = requests.get(url)

# Έλεγχος αν το αίτημα είναι επιτυχές
if response.status_code == 200:
    weather_data = response.json()

    # Αποθήκευση των δεδομένων σε αρχείο JSON
    with open('dubai_weather.json', 'w') as json_file:
        json.dump(weather_data, json_file, indent=4)

    print("Τα δεδομένα καιρού αποθηκεύτηκαν στο 'dubai_weather.json'")
else:
    print(f"Σφάλμα: {response.status_code}")
