import os
import time
import pandas as pd
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")

def get_occupation_type(name: str) -> str:
    """
    Uses OpenAI's ChatCompletion API to classify a person by name
    into one of: Politician, Entrepreneur, or Celebrity.
    """
    messages = [
        {
            "role": "system",
            "content": (
                "You are a knowledgeable assistant that classifies public figures "
                "into exactly one of the following categories: Politician, Entrepreneur, Celebrity."
            )
        },
        {
            "role": "user",
            "content": f"Classify the following individual into one of Politician, Entrepreneur, Celebrity:\n\n{name}"
        }
    ]
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
        )
        text = response.choices[0].message.content.strip()
    except Exception as e:
        print(f"OpenAI API error for {name}: {e}")
        time.sleep(5)
        return get_occupation_type(name)  # retry once

    # Normalize the output to one of the three valid categories
    for category in ["Politician", "Entrepreneur", "Celebrity"]:
        if category.lower() in text.lower():
            return category
    # Fallback
    return "Celebrity"

def classify_csv(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)
    df["occupationType"] = df["name"].apply(get_occupation_type)
    df.to_csv(output_csv, index=False)
    print(f"Classification complete. Saved to {output_csv}")

if __name__ == "__main__":
    classify_csv("cleaned_results.csv", "final.csv")
