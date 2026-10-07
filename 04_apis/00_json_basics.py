import json


# A JSON blob is just a string of text
blob = """
{
  "name": "Ada Lovelace",
  "age": 36,
  "is_student": false,
  "courses": ["DS3022", "DS3001", "CS2100"],
  "address": {
    "city": "Charlottesville",
    "state": "VA",
    "zip": "22903"
  },
  "advisor": null,
  "grades": {
    "DS3022": {"midterm": 91, "final": 88},
    "DS3001": {"midterm": 85, "final": 94}
  }
}
"""

print(type(blob))


# --- Deserialize: JSON text -> Python object ---
data = json.loads(blob)
print(type(data))
print(data)

# --- Simple fields ---



# --- Lists ---



# --- Nested objects ---



# --- Nulls and missing keys ---



# --- Serialize: Python object -> JSON text ---

