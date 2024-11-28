import requests
import json

# URL για στατική πληροφορία σταθμών
url = "https://dubai.publicbikesystem.net/customer/gbfs/v2/en/station_information"
response = requests.get(url)

# Έλεγχος αν το αίτημα είναι επιτυχές
if response.status_code == 200:
    station_info = response.json()
    # Αποθήκευση δεδομένων σε αρχείο JSON
    with open('station_information.json', 'w') as json_file:
        json.dump(station_info, json_file, indent=4)  # Αποθήκευση στο αρχείο
    print("Τα δεδομένα αποθηκεύτηκαν στο 'station_information.json'")
else:
    print(f"Σφάλμα: {response.status_code}")


# URL για δυναμική κατάσταση σταθμών
url = "https://dubai.publicbikesystem.net/customer/gbfs/v2/en/station_status"
response = requests.get(url)

# Έλεγχος αν το αίτημα είναι επιτυχές
if response.status_code == 200:
    station_status = response.json()
    # Αποθήκευση δεδομένων σε αρχείο JSON
    with open('station_status.json', 'w') as json_file:
        json.dump(station_status, json_file, indent=4)  # Αποθήκευση στο αρχείο
    print("Τα δεδομένα αποθηκεύτηκαν στο 'station_status.json'")
else:
    print(f"Σφάλμα: {response.status_code}")
