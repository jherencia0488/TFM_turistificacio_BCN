#Install requested libraries
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd

#Define a function to scrap ids on diferent statistics and download csv from that ids
def process_datasets(dataset_urls):
    """For any url in a list, scrap it an extract de links for download csv files. 
    After that, configures an API for any id and download the data, combines all the csv for any statistic, 
    and save as a combined file in a folder with the name of the statistic."""

    #Iterates in any url of the list to scrap the links to obtain the ids
    for dataset_url in dataset_urls:
        url = f"https://opendata-ajuntament.barcelona.cat/data/ca/dataset/{dataset_url}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            download_links = []
            for link in soup.find_all("a", class_="heading", title=lambda title: title and "csv" in title.lower()):
                download_links.append(link["href"])

            #If there's any id obtained, create a folder with the name of the statistic (element in the list)
            if download_links:
                dataset_name = os.path.basename(dataset_url).split(".")[0]  # Get dataset name from URL

                os.makedirs(dataset_name, exist_ok=True)
                all_dataframes = []
                #For any id configure the API and download the csv file in the folder created
                for link in download_links:
                    resource_id = link.split("/")[-1].split(".")[0]  # Extract resource ID
                    api_url = f"https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?id={resource_id}&limit=50000"
                    try:
                        api_response = requests.get(api_url)
                        api_response.raise_for_status()
                        api_data = api_response.json()

                        if api_data["success"] and "result" in api_data and "records" in api_data["result"]:
                            df = pd.DataFrame(api_data["result"]["records"])
                            all_dataframes.append(df)
                            print(f"Downloaded and processed: {resource_id}")
                        else:
                            print(f"Error: Invalid API response for {resource_id}")

                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching API data for {resource_id}: {e}")
                    except Exception as e:
                        print(f"An error occurred while processing API data for {resource_id}: {e}")
                #If there's csv files downloaded, combines them all in a combined file with the name of the statistic
                if all_dataframes:
                    combined_df = pd.concat(all_dataframes, ignore_index=True)
                    combined_df.to_csv(os.path.join(dataset_name, f"{dataset_name}.csv"), index=False)
                    print(f"Combined CSV for {dataset_name} created successfully.")
                else:
                  print(f"No dataframes were created for {dataset_name}.")
            else:
                print(f"No download links found for {dataset_url}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching the URL {dataset_url}: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

#This is the list of urls (statitistics) to scrap and obtain csv files. You can add or delete any of them if its needed.
dataset_urls = ['pad_mdb_niv-educa-esta_edat-q_sexe',
          'pad_cdo_b_sexe_barri-des',
          'pad_emi_mdbas_sexe_edat-q_continent-m',
          'pad_mdbas_lloc-naix-pais_lloc-naix-continent_sexe',
          'pad_sol_mdb_sexe_edat-q',
          'renda-tributaria-per-persona-atlas-distribucio',
          'atles-renda-index-gini',
          'h2mave-anualt1b',
          'est-cadastre-edificacions-edat-mitjana',
          'est-cadastre-habitatges-edat-mitjana',
          'allotjaments-pensions',
          'allotjaments-hotels',
          'allotjaments-altres',
          'habitatges-us-turistic',
          'cens-locals-planta-baixa-act-economica',
          'cens-activitats-comercials',
          'terrasses-comercos-vigents',
          'equipament-restaurants',
          'patrimoni-arquitectonic-protegit',
          'equipaments-culturals-icub',
          'culturailleure-bibliotequesimuseus',
          'culturailleure-espaismusicacopes',
          'punts-informacio-turistica',
          'dades-festivals',
          'dades-arts-esceniques',
          'dades-museus-exposicions',
          'xarxasoroll-equipsmonitor-dades',
          'qualitat-aire-detall-bcn',
          'aforaments-detall',
          'esm-bcn-evo',
          'culturailleure-espaisparticipaciociutadana']

#Apply the function to the whole list of statistics needed
process_datasets(dataset_urls)
