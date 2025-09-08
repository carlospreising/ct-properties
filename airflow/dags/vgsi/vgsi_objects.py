from dataclasses import dataclass, field
from datetime import datetime
from bs4 import BeautifulSoup
import urllib3
from urllib3.util import parse_url
import requests
import re
import uuid
import sys
import hashlib
from typing import List, Dict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__end_section__ = './Error.aspx?Message=There+was+an+error+loading+the+parcel.'

class InvalidPIDException(Exception):
  pass

@dataclass(kw_only=True)
class Base:

    uuid: str = field(init=False)
    pid: int = field(default=None)
    data: dict | None = None
    _data: dict | None = None
    money_fields: List | None = None
    float_fields: List | None = None
    integer_fields: List | None = None
    datetime_fields: List | None = None
    tag_mapping: Dict = field(default_factory = lambda: {})
    url: str = field(default=None)
    soup: BeautifulSoup = field(default=None)
    updated_at: datetime = field(init=False)

    @property
    def data(self):
        return self._data
    
    @data.setter
    def data(self, new_data: dict):
        if isinstance(new_data, dict):
            for key, value in new_data.items():
                if self.money_fields is not None and key in self.money_fields:
                    new_data[key] = self.handle_money(value)
                elif self.float_fields is not None and key in self.float_fields:
                    new_data[key] = self.handle_float(value)
                elif self.integer_fields is not None and key in self.integer_fields:
                    new_data[key] = self.handle_int(value)
                else:
                    new_data[key] = self.handle_none(str(value))
            self._data = new_data

    def update_data(self, new_data: dict):
        if self._data:
            new_data |= self._data
        self.data = new_data

    @staticmethod
    def handle_money(money_string):

        if isinstance(money_string, str):
            money_value = money_string.strip().replace('$', '').replace(',', '')

            if money_value == '':
                return None
            else:
                return float(money_value)
        elif isinstance(money_string, float):
            return money_string
        else:
            return None
        
    @staticmethod
    def handle_none(column):

        if isinstance(column, str):
            if column.strip() == '':
                return None
        return column
    
    @staticmethod
    def handle_float(float_string):

        if isinstance(float_string, str):
            try:
                return float(float_string.strip())
            except ValueError:
                return None
        elif isinstance(float_string, float):
            return float_string
        else:
            return None
        
    @staticmethod
    def handle_int(int_string):

        try:
            return int(int_string.strip())
        except ValueError:
            return None

    def load_dict(self):

        tag_dict = {}

        for tag in self.soup.find_all('span'):
            try:
                field_name = self.tag_mapping[tag['id']]
                tag_content = tag.get_text(separator = ' ', strip = True)
                tag_dict.update({field_name : tag_content})
            except KeyError:
                pass

        return tag_dict

    def __post_init__(self):
        
        uuid_str = f"{self.pid}{self.data}"
        hex_string = hashlib.md5(uuid_str.encode("UTF-8")).hexdigest()
        self.uuid = str(uuid.UUID(hex=hex_string))
        self.updated_at = datetime.now()

        if self.tag_mapping:
            self.data = self.load_dict()
        
        self.update_data(
            {
                'uuid': self.uuid,
                # 'propery_uuid': self.property_uuid,
                'pid': self.pid,
                'updated_at': self.updated_at
            }
        )

@dataclass(kw_only=True)
class Table(Base):

    property_uuid: str
    row: int = field(default=None)
    table_tag: str = field(default=None)
    tag_mapping: Dict = field(default_factory=lambda: {})

    def load_table_dict(self):

        keys = []
        values = []
        for tag in self.soup.find('table', id=self.table_tag).find_all('tr'):
            for th in tag.find_all('th'):
                key = th.get_text(separator = ' ', strip = True)
                keys.append(key.replace('&', 'and').lower().replace(' ', '_'))

        for tag in self.soup.find('table', id=self.table_tag).find_all('tr')[self.row + 1].find_all('td'):
            value = tag.get_text(separator = ' ')
            values.append(value.replace('&', 'and').lower())

        table_dict = dict(zip(keys, values))

        return table_dict
    
    def __post_init__(self):
        super().__post_init__()
        table_data = self.load_table_dict()
        self.update_data(table_data)

        self.update_data(
            {
                'property_uuid': self.property_uuid
            }
        )

        del self.soup

@dataclass(kw_only=True)
class Building(Table):

    property_uuid: str 
    money_fields: List[str] = field(default_factory=lambda: [
        'replacement_cost', 
        'less_depreciation'
        ]
    )
    float_fields: List[str] = field(default_factory=lambda: ['building_area'])

    def load_table_dict(self):

        table_dict = {}

        table_dict.update({'bid': self.row})

        for tag in self.soup.find('table', id=self.table_tag).find_all('tr'):

            try:
                key = tag.find_all('td')[0].get_text(separator = ' ', strip = True).lower()
                value = tag.find_all('td')[1].get_text(separator = ' ', strip = True)
                table_dict.update({key.replace(' ','_').replace(':','') : value})
            except (KeyError, IndexError):
                pass
        
        return table_dict

    def __post_init__(self):
        if not self.tag_mapping:
            self.tag_mapping = {
                f"MainContent_ctl0{self.row}_lblYearBuilt" : 'year_built',
                f"MainContent_ctl0{self.row}_lblBldArea" : 'building_area',
                f"MainContent_ctl0{self.row}_lblRcn" : 'replacement_cost',
                f"MainContent_ctl0{self.row}_lblRcnld" : "less_depreciation"
            }
        
        if self.table_tag:
            self.table_tag = self.table_tag.format(str(self.row))
        else:
            self.table_tag = f"MainContent_ctl0{self.row}_grdCns"

        super().__post_init__()

@dataclass(kw_only=True)
class Ownership(Table):

    table_tag: str = field(default="MainContent_grdHistoryValuesAppr")
    money_fields: List[str] = field(default_factory=lambda: ['sale_price'])

@dataclass(kw_only=True)
class Appraisal(Table):

    table_tag: str
    money_fields: List[str] = field(default_factory=lambda:[
        'improvements', 
        'land', 
        'total'
        ]
    )

@dataclass(kw_only=True)
class Property(Base):

    money_fields: List[str] = field(default_factory=lambda:[
        'sale_price', 
        'assesment_value', 
        'appraisal_value', 
        'land_assessed_value', 
        'land_appraised_value'
        ]
    )
    city: str = field(default='newhaven')
    state: str = field(default='ct')
    ownership: List = field(default_factory=lambda: [])
    buildings: List = field(default_factory=lambda: [])
    appraisals: List = field(default_factory=lambda: [])
    assesments: List = field(default_factory=lambda: [])
    longitude: float = field(default=None)
    latitude: float = field(default=None)
    url: str = field(default=None)
    city_url: str = field(default=None)
    soup: BeautifulSoup = field(default = None)
    tag_mapping: Dict = field(default_factory=lambda: {
            "MainContent_lblPid": "pid",
            "MainContent_lblAcctNum": "account_number",
            "lblTownName": "town_name",
            "MainContent_lblLocation": "address",
            "MainContent_lblGenOwner": "owner",
            "MainContent_lblAddr1": "owner_address",
            "MainContent_lblCoOwner": "co_owner",
            "MainContent_lblPrice": "sale_price",
            "MainContent_lblCertificate": "certificate",
            "MainContent_lblSaleDate": "sale_date",
            "MainContent_lblBp": "book_page",
            "MainContent_lblBookLabel": "book_label",
            "MainContent_lblBook": "book",
            "MainContent_lblPageLabel": "page_label",
            "MainContent_lblPage": "page",
            "MainContent_lblInstrument": "label_instrument",
            "MainContent_lblGenAssessment": "assesment_value",
            "MainContent_lblGenAppraisal": "appraisal_value",
            "MainContent_lblBldCount": "building_count",
            "MainContent_lblUseCodeDescription": "building_use",
            "MainContent_lblAltApproved": "land_alt_approved",
            "MainContent_lblUseCode": "land_use_code",
            "MainContent_lblZone": "land_zone",
            "MainContent_lblNbhd": "land_neighborhood_code",
            "MainContent_lblLndAcres": "land_size_acres",
            "MainContent_lblLndFront": "land_frontage",
            "MainContent_lblDepth": "land_depth",
            "MainContent_lblLndAsmt": "land_assessed_value",
            "MainContent_lblLndAppr": "land_appraised_value"
        }
    )
    ownership_table_tag: str = field(default='MainContent_grdSales')
    appraisal_table_tag: str = field(default='MainContent_grdHistoryValuesAppr')
    assesment_table_tag: str = field(default='MainContent_grdHistoryValuesAsmt')
    building_table_tag: str = field(default="MainContent_ctl0{}_grdCns")

    def add_building(self, bid):

        # sys.stdout.write(f'Adding building {bid} to property {self.pid}. \n')s
        building = Building(
            pid=self.pid, 
            property_uuid=self.uuid, 
            row=bid,
            soup=self.soup,
            table_tag=self.building_table_tag
        )

        self.buildings.append(building.data)

    def add_ownership(self, row):

        # sys.stdout.write(f'Adding property ownership to property {self.pid}. \r')
        owner = Ownership(
            pid=self.pid, 
            property_uuid=self.uuid, 
            row=row, 
            soup=self.soup,
            table_tag=self.ownership_table_tag
        )

        self.ownership.append(owner.data)
    
    def add_assesment(self, row):

        # sys.stdout.write(f'Adding property assesments to property {self.pid}. \r')
        assesment = Appraisal(
            pid=self.pid, 
            property_uuid=self.uuid, 
            row=row, 
            soup=self.soup,
            table_tag=self.assesment_table_tag
        )
    
        self.assesments.append(assesment.data)

    def add_appraisal(self, row):

        # sys.stdout.write(f'Adding appraisals assesments to property {self.pid}. \r')
        appraisal = Appraisal(
            pid=self.pid, 
            property_uuid=self.uuid, 
            row=row, 
            soup=self.soup,
            table_tag=self.appraisal_table_tag
        )

        self.appraisals.append(appraisal.data)

    def load_assesment(self):
        try:
            assesments_count = len(self.soup.find('table', id=self.assesment_table_tag).find_all('tr')) - 1
            # print(f"Assesment count {assesments_count}")
            for row in range(0, assesments_count):
                self.add_assesment(row)
                
        except KeyError:
            raise Warning(
                """
                Null assesments history.
                """
            )

    def load_appraisal(self):
        try:
            appraisal_count = len(self.soup.find('table', id=self.appraisal_table_tag).find_all('tr')) - 1
            for row in range(0, appraisal_count):
                self.add_appraisal(row)
                
        except KeyError:
            raise Warning(
                """
                Null appraisal history.
                """
            )

    def load_ownership(self):
        try:
            ownership_count = len(self.soup.find('table', id=self.ownership_table_tag).find_all('tr')) - 1
            for row in range(0, ownership_count):
                self.add_ownership(row)
                
        except KeyError:
            raise Warning(
                """
                Null ownership history.
                """
            )

    def load_buildings(self):
        try:
            building_count = self.handle_int(self.data['building_count'])
            for bid in range(0, building_count+3):
                try:
                    self.add_building(bid)
                except:
                    pass
                
        except KeyError:
            raise Warning(
                """
                Null building count.
                """
            )
    
    def load_all(self):

        # sys.stdout.write(f'Adding all subclasses to property {self.pid}. \r')
        try:
            self.load_buildings()
        except:
            print("Could not load buildings.")
        try:
            self.load_assesment()
        except:
            print("Could not load assesments.")
        try:
            self.load_appraisal()
        except:
            print("Could not load appraisals")
        try:
            self.load_ownership()
        except:
            print("Could not load ownership.")

        del self.soup

    def __post_init__(self):

        url = parse_url(self.url)
        if self.pid is None:
            match = re.search(r'(pid=)([\d]+)', str(url.query))
            if match:
                pid = int(match.group(2))
                self.pid = pid
            else:
                raise Exception(
                    """
                    You must instantiate this class with either a base url and property id 
                    ex: (url='https://gis.vgsi.com/newhavenct/', pid=100542)
                    or a url with the property id
                    ex: (url='https://gis.vgsi.com/newhavenct/Parcel.aspx?pid=82')
                    """
                )
            page = requests.get(str(url), verify=False)
        else:
            page = requests.get(str(url) + 'Parcel.aspx?pid=' + str(self.pid) , verify=False)
        
        soup = BeautifulSoup(page.content, "html.parser")
        self.soup = soup
        self.url = url
        
        try:
            if self.soup.find(id="form1")['action'] == __end_section__:
                raise InvalidPIDException(
                    """
                    PID doesn't return housing data.
                    """
                )
        except AttributeError:
                raise InvalidPIDException(
                    """
                    PID doesn't return housing data.
                    """
                )
        
        super().__post_init__()