from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from clickhouse_sqlalchemy import Table, engines
from clickhouse_driver import Client
from Local_Click_House_DB_Context import Local_Click_House_DB_Context

# ip to region data library
import requests
import numpy
import json
import random
import socket
import struct


import pandas as pd
import numpy as np
import re
from datetime import date
import datetime
import calendar

from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression

import tensorflow as tf
from tensorflow.keras.layers import Dense, Flatten
from tensorflow.keras import Model


import hashlib
import os
import pickle


def hash_trick(column, output_dim = 10 ) :
    h = hashlib.md5()
    return_array = np.zeros((column.shape[0],int(output_dim)))
    for i, row in enumerate(column) :
        hashed_value = int(hashlib.md5(row.encode('UTF-8')).hexdigest(),16) % output_dim
        return_array[i,hashed_value] += 1
    return return_array

def load_encoder() :
    encoder_dict = {}
    with open(os.getcwd()+'/encoder_folder/MEDIA_CATE_ENC.bin', 'rb') as f:
        encoder_dict['MEDIA_CATE_INFO'] = pickle.load(f)

    with open(os.getcwd()+'/encoder_folder/SCRIPT_TP_ENC.bin', 'rb') as f:
        encoder_dict['SCRIPT_TP_CODE'] = pickle.load(f)

    with open(os.getcwd()+'/encoder_folder/MONTH_CATE_ENC.bin', 'rb') as f:
        encoder_dict['MONTH_CATE'] = pickle.load(f)

    with open(os.getcwd()+'/encoder_folder/WEEK_CATE_ENC.bin', 'rb') as f:
        encoder_dict['WEEK'] = pickle.load(f)

    with open(os.getcwd()+'/encoder_folder/STATS_HH_ENC.bin', 'rb') as f:
        encoder_dict['STATS_HH'] = pickle.load(f)

    with open(os.getcwd()+'/encoder_folder/MINUTE_BINS_ENC.bin', 'rb') as f:
        encoder_dict['MINUTE_BINS'] = pickle.load(f)

    with open(os.getcwd()+'/encoder_folder/PLTFOM_TP_CODE_ENC.bin', 'rb') as f:
        encoder_dict['PLTFOM_TP_CODE'] = pickle.load(f)
    return encoder_dict


def preprocess_sample_data(data_extract_context, table_name, start_dttm, last_dttm, encoder_dict,
                           data_cnt=100000, click_ratio = 0.1, targeting = 'AD'):
    sample_data = data_extract_context.Extract_Sample_Log(table_name, start_dttm, last_dttm, targeting=targeting,
                                                          sample_cnt=data_cnt)
    sample_data = sample_data[['STATS_DTTM', 'STATS_HH', 'STATS_MINUTE', 'MEDIA_SCRIPT_NO',
                               'ADVRTS_PRDT_CODE', 'ADVRTS_TP_CODE', 'ADVER_ID',
                               'PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE', 'CLICK_YN']]
    sample_data = pd.merge(sample_data, data_extract_context.Media_Property_Df, on=['MEDIA_SCRIPT_NO'])
    sample_data = pd.merge(sample_data,
                           data_extract_context.Adver_Cate_Df[['ADVER_ID', 'CTGR_SEQ_3', 'CTGR_SEQ_2', 'CTGR_SEQ_1']],
                           on=['ADVER_ID'])
    sample_data['STATS_DTTM'] = sample_data['STATS_DTTM'].astype('str')
    sample_data['WEEK'] = sample_data['STATS_DTTM'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y%m%d').strftime('%a'))
    sample_data['MONTH_CATE'] = sample_data['STATS_DTTM'].apply(
        lambda x: 1 if int(x[-2:]) < 10 else (2 if int(x[-2:]) > 20 else 3))
    sample_data['MINUTE_BINS'] = \
    pd.cut(sample_data['STATS_MINUTE'], [0, 9, 19, 29, 39, 49, 59], labels=False, retbins=True, right=False)[0]
    sample_data['SCRIPT_TP_CODE'] = sample_data['SCRIPT_TP_CODE'].apply(lambda x: '03' if x == '14' else x)
    count_per_feature = sample_data[['MONTH_CATE', 'PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE']].groupby(
        ['PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE']).count().reset_index()
    count_per_feature['DELETE_YN'] = count_per_feature['MONTH_CATE'].apply(lambda x: 0 if x < 10 else 1)

    sample_data = pd.merge(sample_data, count_per_feature[['PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE', 'DELETE_YN']],
                           on=['PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE'])

    sample_data = sample_data[sample_data['DELETE_YN'] == 1]
    sample_data['BROWSER_CODE'] = sample_data['BROWSER_CODE'].apply(lambda x: 'vacant' if x == '' else x)
    sample_data['OS_CODE'] = sample_data['OS_CODE'].apply(lambda x: 'vacant' if x == '' else x)
    sample_data['ADVER_MEDIA_HASH_KEY'] = sample_data['ADVER_ID'] + '_' + sample_data['MEDIASITE_NO']

    sample_data.dropna(inplace=True)
    sample_data[['WIDTH', 'HEIGHT']] = sample_data[['WIDTH', 'HEIGHT']] / 1000

    click_cnt = sample_data[sample_data['CLICK_YN']==1].shape[0]
    print(click_cnt)
    view_cnt = int(click_cnt/click_ratio)
    print(view_cnt)

    click_data = sample_data[sample_data['CLICK_YN'] == 1]
    try :
        view_data = sample_data[sample_data['CLICK_YN']==0].sample(n=view_cnt)
    except :
        view_data = sample_data[sample_data['CLICK_YN']==0]
    print(view_data.shape)
    subsample_data = pd.concat([view_data,click_data])
    print(subsample_data.shape)
    subsample_data = subsample_data.sample(frac = 1)

    encoder_dict = load_encoder()

    encode_column_list = ['MONTH_CATE', 'WEEK', 'STATS_HH', 'PLTFOM_TP_CODE']

    encoded_array_list = []
    for column in encode_column_list:
        print(column)
        encoded_array = encoder_dict[column].transform(subsample_data[[column]]).toarray()
        encoded_array_list.append(encoded_array)

    encoded_array_list.append(hash_trick(subsample_data[['MEDIA_CATE_INFO']], 20))
    encoded_array_list.append(hash_trick(subsample_data[['SCRIPT_TP_CODE']], 25))
    encoded_array_list.append(hash_trick(subsample_data[['ADVER_MEDIA_HASH_KEY']],100))
    # encoded_array_list.append(hash_trick(input_data[['BROWSER_CODE']], 20))
    # encoded_array_list.append(hash_trick(input_data[['OS_CODE']], 15))
    encoded_array_list.append(subsample_data[['WIDTH']].values)
    encoded_array_list.append(subsample_data[['HEIGHT']].values)
    encoded_array_list.append(subsample_data[['SIZE_RATIO']].values)

    input_data = np.hstack(encoded_array_list)

    output_data = subsample_data[['CLICK_YN']].values
    return input_data, np.squeeze(output_data)

def Return_NN_Model(input_shape, node_list) :
    layer_list = []
    for node_count in node_list :
        layer_list.append(tf.keras.layers.Dense(node_count, activation='relu'))
        layer_list.append(tf.keras.layers.Dropout(0.2))
    layer_list.insert(0,tf.keras.layers.Dense(input_shape,activation='relu'))
    layer_list.insert(-1,tf.keras.layers.Dense(2,activation='softmax'))
    model = tf.keras.models.Sequential(layer_list)
    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])
    return model


if __name__ == '__main__' :
    clickhouse_id = "analysis"
    clickhouse_password = "analysis@2020"
    local_clickhouse_id = "click_house_test1"
    local_clickhouse_password = "0000"
    local_clickhouse_ip = "192.168.100.237:8123"
    local_clickhouse_DB_name = "TEST"
    local_clickhouse_Table_name = 'CLICK_VIEW_YN_LOG_NEW'
    test_context = Local_Click_House_DB_Context(local_clickhouse_id, local_clickhouse_password,
                                                local_clickhouse_ip, local_clickhouse_DB_name,
                                                local_clickhouse_Table_name)

    # model = LogisticRegression(penalty='l2')
    encoder_dict = load_encoder()

    for i in range(1) :
        vali_input_data, vali_output_data = preprocess_sample_data(test_context, 'CLICK_VIEW_YN_LOG_NEW',
                                                                   '20201001','20201019', encoder_dict, 100000, 0.0001)
        vali_input_data, vali_output_data =  vali_input_data.astype(np.float32), vali_output_data.astype(np.float32)
        print("validation data extracted")

    model = Return_NN_Model(vali_input_data.shape[1], [50, 10, 10])

    checkpoint_path = "training_1/cp.ckpt"
    checkpoint_dir = os.path.dirname(checkpoint_path)
    cp_callback = tf.keras.callbacks.ModelCheckpoint(filepath=checkpoint_path,
                                                     save_weights_only = True,
                                                     verbose=1)

    for i in range(10):
        print("train sequence {0}".format(i))
        input_data, output_data = preprocess_sample_data(test_context, 'CLICK_VIEW_YN_LOG_NEW',
                                                         '20201001', '20201019', encoder_dict, 1000000,0.0001)
        input_data, output_data  = input_data.astype(np.float32), output_data.astype(np.float32)
        print(input_data.shape, output_data.shape)
        model.fit(input_data,
                  output_data,
                  epochs=5,
                  validation_data=(vali_input_data, vali_output_data),
                  callbacks=[cp_callback])

    # 훈련된 모델 파라미터 불러오는 방법.
    # new_model = Return_NN_Model(vali_input_data.shape[1], [50, 10, 10])
    # new_model.load_weights(checkpoint_path)



    # print(input_data.shape, output_data.shape)
    # input_shape = input_data.shape[0]
    # print(input_shape)
    #
    #
    # model = MyModel(input_shape)
    # loss_object = tf.keras.losses.SparseCategoricalCrossentropy()
    # optimizer = tf.keras.optimizers.Adam()
    #
    # test_input_data, test_output_data = preprocess_sample_data(test_context, 'CLICK_VIEW_YN_LOG_NEW', '20201001', '20201010',
    #                                                            encoder_dict, 100000)
    # predict_prob_y = model.predict_proba(test_input_data)
    # CTR = predict_prob_y[:, 1] * 100
    # print(CTR)
