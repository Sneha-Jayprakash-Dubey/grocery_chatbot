import json
import random

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB

with open("intents.json", encoding="utf-8") as file:
    data = json.load(file)

sentences=[]
labels=[]

for intent in data["intents"]:
    for pattern in intent["patterns"]:
        sentences.append(pattern)
        labels.append(intent["tag"])

vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(sentences)

model = MultinomialNB()
model.fit(X,labels)

def chatbot_response(msg):

    X_test = vectorizer.transform([msg])
    tag = model.predict(X_test)[0]

    for intent in data["intents"]:
        if intent["tag"]==tag:
            return random.choice(intent["responses"])

    return "Sorry I didn't understand."
