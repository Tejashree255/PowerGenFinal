
import numpy as np
import streamlit as st
import numpy as np 
import tensorflow as tf
from tensorflow import keras
from keras.models import load_model

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
st.subheader('Daily generation Forecasting')
def makeXy(ts, time_steps):
    X= []
    y= []
    for i in range(time_steps, ts.shape[0]):
        #print(ts.loc[i-time_steps:i-1])
        X.append(list(ts.loc[i-time_steps:i-1]))
        y.append(ts.loc[i])
    X,y = np.array(X), np.array(y)
    return X,y

wtgNo = st.selectbox("Select Wind Turbine Location No. ", ("GP31", "GP102", "GP101", "K239", "K289", "K437", "M496", "M481", "S52",
                                                           "WPPLCV-144", "JFPLJD-37", "WPPLJD-38", "JFCV-05(142)", "JFCV-06(143)"))

if wtgNo == "GP31":
    #Generation_GP31.04-2670535.7500.hdf5
    model = load_model("Generation_GP31.04-2670535.7500.hdf5")
elif wtgNo == "GP102":
    model = load_model("Generation_GP102.30-1986206.7500.hdf5")
elif wtgNo == "GP101":
    model = load_model("Generation_GP101.22-2320074.7500.hdf5")
elif wtgNo == "K239":
    model = load_model("Generation_K239.16-8400515.0000.hdf5")
elif wtgNo == "K289":
    model = load_model("Generation_K289.18-8820013.0000.hdf5")
elif wtgNo == "K437":
    model = load_model("Generation_K437.19-8813438.0000.hdf5")
elif wtgNo == "M496":
    model = load_model("Generation_M496.23-2947009.7500.hdf5")
elif wtgNo == "M481":
     model = load_model("Generation_M481.52-2931480.5000.hdf5")
elif wtgNo == "S52":
     model = load_model("Generation_S52.28-17084604.0000.hdf5")
elif wtgNo == "WPPLCV-144":
     model = load_model("Generation_WPPLCV.09-2502306.0000.hdf5")
elif wtgNo == "JFPLJD-37":
    model = load_model("Generation_JFPLJD.43-8819256.0000.hdf5")
elif wtgNo == "JFCV-05(142)":
    model = load_model("Generation_JFCV-05.05-4673578.0000.hdf5")
elif wtgNo == "JFCV-06(143)":
     model = load_model("Generation_JFCV-06.29-4981943.5000.hdf5")
else:
    st.write("Please select valid wind turbine location number")

day1 = st.number_input("Day 1 generation")
day2 = st.number_input("Day 2 generation")
day3 = st.number_input("Day 3 generation")
day4 = st.number_input("Day 4 generation")
day5 = st.number_input("Day 5 generation")
day6 = st.number_input("Day 6 generation")
day7 = st.number_input("Day 7 generation")

x_input = np.array([day1, day2, day3, day4, day5, day6, day7])
x_input = x_input.reshape((1, 7))
result=st.button('Predict')

if result:
    yPred = model.predict(x_input)
    st.write("Day 8 prediction:- ")
    y = yPred[0][0]
    st.write(y)
