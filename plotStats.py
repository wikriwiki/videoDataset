import pandas as pd
import matplotlib.pyplot as plt




def plot():
    df = pd.read_csv("final.csv")

    counts = df["occupationType"].value_counts()

    print("Occupation Type Counts: ")
    print(counts)

    plt.figure()
    counts.plot.pie(
        autopct ="%1.1f%%",
        startangle= 90,
        ylabel="",
    )
    plt.title("Occupation Type Distribution")
    plt.show()
if __name__ == "__main__":
    plot()