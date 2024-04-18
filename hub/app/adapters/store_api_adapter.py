import json
import logging
from typing import List
import requests
from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url, buffer_size=10):
        self.api_base_url = api_base_url  # Базовий URL API
        self.buffer_size = buffer_size  # Розмір буфера
        self.buffer: List[ProcessedAgentData] = []  # Буфер

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        """
        Зберегти оброблені дані про дорогу до API магазину.
        Параметри:
            processed_agent_data_batch (list): Список оброблених даних про дорогу, які потрібно зберегти.
        Повертає:
            bool: True, якщо дані успішно збережені, False в іншому випадку.
        """
        # Реалізуємо функціонал
        try:
            # Додаємо дані пакету до буфера
            self.buffer.extend(processed_agent_data_batch)
            if len(self.buffer) >= self.buffer_size:
                success = self.send_data(
                    self.buffer
                )  # Якщо досягнуто розміру буфера, відправляємо дані
                # Очищення буфера після відправлення
                self.buffer.clear()
                return success
            else:
                return True
        except Exception as e:
            logging.error(f"Виникла помилка: {e}")
            return False

    def processed_agent_data_list_to_list_of_dict(
        self, data_list: List[ProcessedAgentData]
    ):
        """
        Перетворення списку ProcessedAgentData на список словників.
        """
        processed_data = []
        for data in data_list:
            # Перетворення ProcessedAgentData у словник
            processed_data.append(data.model_dump())

        for data in processed_data:
            # Серіалізація мітки часу у формат ISO
            data["agent_data"]["timestamp"] = data["agent_data"][
                "timestamp"
            ].isoformat()

        return processed_data

    def send_data(self, data: List[ProcessedAgentData]) -> bool:
        """
        Відправлення накопичених даних до API магазину.
        """
        url = f"{self.api_base_url}/processed_agent_data/"
        try:
            json_data = self.processed_agent_data_list_to_list_of_dict(data)
            # Відправлення POST-запиту на API
            response = requests.post(url, json=json_data)
            if response.ok:  # Перевірка успішності запиту
                return True
            else:
                logging.error(
                    f"Failed to save data. Error: {response.status_code}"
                )
                return False
        except Exception as e:
            logging.error(f"Error ocured: {e}")
            return False