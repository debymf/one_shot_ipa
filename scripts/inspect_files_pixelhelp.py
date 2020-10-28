import tensorflow as tf
from google.protobuf.json_format import MessageToJson, MessageToDict
import json
from tqdm import tqdm
A_VERY_LARGE_NUMBER = 100000000000
feature_list = [("goal", "int64_list"), ("input_str_position_seq", "int64_list"), ("instruction_char_id_seq", "int64_list"),
                ("instruction_rule_id", "int64_list"), ("instruction_str",
                                                        "bytes_list"), ("instruction_word_id_seq", "int64_list"),
                ("obj_desc_position_seq", "int64_list"), ("package_name",
                                                          "bytes_list"), ("package_name_id", "int64_list"),
                ("raw_goal", "bytes_list"), ("task_id",
                                             "int64_list"), ("ui_obj_clickable_seq", "int64_list"),
                ("ui_obj_cord_x_seq", "float_list"),
                ("ui_obj_cord_y_seq",
                 "float_list"),  ("ui_obj_dom_distance", "int64_list"),
                ("ui_obj_dom_location_seq", "int64_list"),
                ("ui_obj_h_distance", "float_list"),
                ("ui_obj_str_seq", "bytes_list"),
                ("ui_obj_type_id_seq", "int64_list"),
                ("ui_obj_v_distance", "float_list"), ("ui_obj_word_id_seq", "int64_list"),
                ("ui_target_id_seq", "int64_list"), ("verb_id_seq", "int64_list")]


total_values = 0
parsed_tf_record = dict()
for i in tqdm(range(0, 10)):
    raw_dataset = tf.data.TFRecordDataset(f"pixel_help_{i}.tfrecord")
    for raw_record in raw_dataset.take(A_VERY_LARGE_NUMBER):
        example = tf.train.Example()
        example.ParseFromString(raw_record.numpy())
        parsed_tf_record[total_values] = dict()
        for feat in feature_list:
            name_feature = feat[0]
            type_feature = feat[1]
            if "int64" in type_feature:
                feature_values = list(
                    example.features.feature[name_feature].int64_list.value)
            elif "float" in type_feature:
                feature_values = list(
                    example.features.feature[name_feature].float_list.value)

            else:
                feature_values_bytes = list(
                    example.features.feature[name_feature].bytes_list.value)

                feature_values = [f.decode('utf-8')
                                  for f in feature_values_bytes]

            parsed_tf_record[total_values][name_feature] = feature_values

        total_values = total_values + 1

        if total_values == 5:
            with open("pixel_help_5_examples.json", "w") as f:
                json.dump(parsed_tf_record, f)


print(f"TOTAL: {total_values}")

with open("pixel_help.json", "w") as f:
    json.dump(parsed_tf_record, f)
