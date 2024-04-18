from csv import reader, DictReader

from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
from marshmallow import Schema
from schema.accelerometer_schema import AccelerometerSchema
from schema.gps_schema import GpsSchema

from datetime import datetime
import config
from enum import Enum

class DataType(Enum):
    """Enum для різних типів даних."""
    ACCELEROMETER = 1
    GPS = 2

class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
    ) -> None:
        """Ініціалізує FileDatasource з іменами файлів для даних акселерометра та GPS."""
        # Ініціалізація словника для зберігання зчитувачів для різних типів даних
        self.readers = dict()

        # Створення зчитувача GPS-даних і збереження їх у словнику
        self.readers[DataType.GPS] = CSVDatasourceReader(
            gps_filename, GpsSchema()
        )

        # Створення зчитувача даних з акселерометра та збережіть їх у словнику
        self.readers[DataType.ACCELEROMETER] = CSVDatasourceReader(
            accelerometer_filename, AccelerometerSchema()
        )

    def read(self) -> AggregatedData:
        """Повертає агреговані дані, отримані з датчиків."""
        try:
            # Зчитування даних акселерометра та GPS
            acc = self.readers[DataType.ACCELEROMETER].read()
            gps = self.readers[DataType.GPS].read()

            # Отримання поточної мітки часу та ідентифікатора користувача
            ts = datetime.now()
            id = config.USER_ID

            # Повернення агрегованих даних
            return {
            "user_id": id,
            "accelerometer": acc,
            "gps": gps,
            "timestamp": ts
        }
        except Exception as e:
            print(f"Reading data from sensors || Error: {e}")

    def startReading(self, *args, **kwargs):
        """Викликається перед початком читання даних."""
        # Початок зчитування даних для всіх датчиків
        for reader in self.readers.values():
            reader.startReading()

    def stopReading(self, *args, **kwargs):
        """Викликається в кінці читання даних."""
        # Кінець зчитування даних для всіх датчиків
        for reader in self.readers.values():
            reader.stopReading()


class CSVDatasourceReader:
    """Клас для читання даних з CSV файлів."""
    filename: str
    reader: DictReader

    def __init__(self, filename, schema: Schema):
        """Ініціалізація зчитувача джерел даних CSV ім'ям файлу та схемою."""
        self.filename = filename
        self.schema = schema

    def startReading(self):
        """Відкривання файлу CSV та ініціалізація зчитувача."""
        self.file = open(self.filename, 'r')
        self.reader = DictReader(self.file)

    def read(self):
        """Читання рядку даних з CSV-файлу."""
        row = next(self.reader, None)

        # Якщо досягнуто кінця файлу, перезавантажте зчитувач, щоб почати з початку
        if row is None:
            self.reset()
            row = next(self.reader, None)

        # Завантаження данних рядка за вказаною схемою
        return self.schema.load(row)

    def reset(self):
        """Перезавантажує зчитувач, щоб почати з початку файлу."""
        self.file.seek(0)
        self.reader = DictReader(self.file)

    def stopReading(self):
        """Закриває файл CSV."""
        if self.file:
            self.file.close()