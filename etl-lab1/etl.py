import glob
import pandas as pd
import xml.etree.ElementTree as ET

#extraction

def exract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    print(dataframe)
    return dataframe


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height": height, "weight": weight}])], ignore_index=True)
    return dataframe


def extract(path):
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])
    
    #process all csv files
    for csvfile in glob.glob(f"{path}/sources/*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(exract_from_csv(csvfile))], ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob(f"{path}/source/*.json"):
        extract_from_json = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)
        
    #process all xml files
    for xmlfile in glob.glob(f"{path}/source/*.xml"):
        extract_from_xml = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)
        
    return extracted_data


#tranformation

def transform(data):
    #Convert inches to meters and round off to two decimals
    data['height'] = round(data.height * 0.0254, 2)
    
    #Convert pounds to kilograms and round off to two decimals
    data['weight'] = round(data.weight * 0.45359237, 2)
    
    return data


#loading

def load(target_file, data: pd.DataFrame):
    data.to_csv(target_file)