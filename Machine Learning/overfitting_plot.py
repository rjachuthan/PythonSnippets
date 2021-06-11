from sklearn import tree
from sklearn import metrics

# For plotting
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns

# Global size of Label Text on the plots
matplotlib.rc("xtick", labelsize=20)
matplotlib.rc("ytick", labelsize=20)

# Ensure that the plot is displayed inside the notebook
# %matplotlib inline

train_accuracies = [0.5]
test_accuracies = [0.5]

for depth in range(1, 5):
    clf = tree.DecisionTreeClassifier(max_depth=depth)

    cols = [
        "fixed aciditity",
        "volatile acidity",
        "citric acid",
        "residual sugar",
        "chlorides",
        "free sulfur dioxide",
        "total sulfur dioxide",
        "density",
        "pH",
        "sulphates",
        "alcohol"
    ]

    # Fit the model on given features
    clf.fit(df_train[cols], df_train.quality)

    # creating training and test predicitons
    train_predictions = clf.predict(df_train[cols])
    test_predictions = clf.predict(df_test[cols])

    # calculate training and test accuracies
    train_accuracy = metrics.accuracy_score(df_train.quality, train_predictions)
    test_accuracy = metrics.accuracy_score(df_test.quality, test_predictions)

    # append accuracies
    train_accuracies.append(train_accuracy)
    test_accuracies.append(test_accuracy)


# Create two plots using matplotlib and seborn
plt.figure(figsize=(10, 5))
sns.set_style("whitegrid")
plt.plot(train_accuracies, label="Train Accuracy")
plt.plot(test_accuracies, label="Test Accuracy")
plt.legent(loc="Upper Left", prop={"size": 15})
plt.xticks(range(0, 26, 5))
plt.xlabel("max_depth", size=20)
plt.ylabel("accuracy", size=20)
plt.show()

