# 🧰 Flattener

**Flattener** is a lightweight utility to recursively flatten nested JSON structures, especially useful in Spark or data engineering pipelines. It handles nested structs, arrays, and complex schemas often found in deeply nested JSONs (like those from REST APIs or insurance policy data).

---

## 🚀 Features

* Flattens nested JSON structures into a flat schema.
* Supports:

  * Nested structs (`StructType`)
  * Arrays of structs
  * Arrays of primitive types
* Works with PySpark DataFrames.
* Configurable field prefixing.

---

## 📆 Installation

Clone the repository:

```bash
git clone https://github.com/rohan-databricks/flattener.git
cd flattener
```

Use directly in your PySpark project by importing the `flattener.py`.

---

## 📄 Usage

### Sample JSON Schema (nested)

```json
{
  "policy": {
    "id": "P123",
    "holder": {
      "name": "John Doe",
      "age": 35
    },
    "coverages": [
      {
        "type": "auto",
        "limit": 100000
      }
    ]
  }
}
```

### Example in PySpark

```python
from pyspark.sql import SparkSession
from flattener import flatten_df

spark = SparkSession.builder.getOrCreate()

# Load your JSON
df = spark.read.json("sample.json")

# Flatten it
flat_df = flatten_df(df)

flat_df.show(truncate=False)
```

---

## 🧠 How It Works

* Recursively traverses the schema.
* Struct fields are expanded into individual columns with a prefix.
* Arrays of structs are exploded and flattened.
* Array of primitives is retained or exploded (depending on config).

---

## 🛠️ Configuration

The `flatten_df()` function supports these optional parameters:

| Param     | Type      | Description                      |
| --------- | --------- | -------------------------------- |
| `df`      | DataFrame | Input nested DataFrame           |
| `prefix`  | str       | Prefix to prepend to field names |
| `explode` | bool      | Whether to explode arrays        |

---

## ✅ Example Output

| policy\_id | policy\_holder\_name | policy\_holder\_age | policy\_coverages\_type | policy\_coverages\_limit |
| ---------- | -------------------- | ------------------- | ----------------------- | ------------------------ |
| P123       | John Doe             | 35                  | auto                    | 100000                   |

---

## 📁 Project Structure

```
flattener/
├── flattener.py
├── test_flattener.py
├── sample_data.json
├── README.md
```

---

## 🧪 Testing

```bash
pytest test_flattener.py
```

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or submit a pull request.

---

## 📄 License

Open Source
