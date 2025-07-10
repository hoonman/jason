import pandas as pd

dataframe = pd.DataFrame(
    {
        "Name": [
            "Braud, Mr. Owen Harris",
            "Allen, Mr. William henry",
            "Bonnell, Miss. Elizabeth",
        ],
        "Age": [22, 35, 58],
        "Sex": ["male", "male", "female"],
    }
)

print("data frame: \n", dataframe)
print("age in data frame: \n", dataframe["Age"])

print("max age: \n", dataframe["Age"].max())
print("describe dataframe: \n", dataframe.describe())