import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random


class InboundDataGenerator:
    def __init__(self, seed=42):
        self.seed = seed
        np.random.seed(seed)
        random.seed(seed)

        # Configurações principais
        self.total_operations = 1000
        self.total_sellers = 100  # Total de sellers
        self.date_range = pd.date_range("2024-01-15", "2024-02-02", freq="D")

        # Distribuição de sellers
        self.state_distribution = {
            "São Paulo": 0.60,  # 60 sellers
            "Minas Gerais": 0.25,  # 25 sellers
            "Santa Catarina": 0.10,  # 10 sellers
            "Bahia": 0.05,  # 5 sellers
        }

        # Distribuição de status
        self.status_distribution = {
            "delivered": 0.75,
            "in_progress": 0.10,
            "cancelled": 0.08,
            "delayed": 0.05,
            "scheduled": 0.02,
        }

        # Centros de Distribuição (CDs) por estado
        self.warehouses_by_state = {
            "São Paulo": ["WH01", "WH02", "WH03", "WH04"],
            "Minas Gerais": ["WH05", "WH06"],
            "Santa Catarina": ["WH07"],
            "Bahia": ["WH08"],
        }

        # Distribuição de CDs baseada na capacidade
        self.warehouse_distribution = {
            "WH01": 0.20,  # BR-SP-CAJ - maior capacidade
            "WH02": 0.18,  # BR-SP-ARA
            "WH03": 0.12,  # BR-SP-FRA
            "WH04": 0.10,  # BR-SP-GRU
            "WH05": 0.15,  # BR-MG-BET
            "WH06": 0.10,  # BR-MG-EXT
            "WH07": 0.10,  # BR-SC-GOV
            "WH08": 0.05,  # BR-BA-LAU - menor capacidade
        }

        # Horários de agendamento
        self.time_slots = [
            "08:00",
            "08:30",
            "09:00",
            "09:30",
            "10:00",
            "10:30",
            "11:00",
            "11:30",
            "12:00",
            "12:30",
            "13:00",
            "13:30",
            "14:00",
            "14:30",
            "15:00",
            "15:30",
            "16:00",
        ]

    def generate_facilities_data(self):
        """Gera dados das facilities"""
        facilities_data = {
            "SHP_FACILITY_ID": [
                "WH01",
                "WH02",
                "WH03",
                "WH04",
                "WH05",
                "WH06",
                "WH07",
                "WH08",
            ],
            "SHP_FACILITY_TYPE": [
                "warehouse",
                "warehouse",
                "receiving_center",
                "warehouse",
                "receiving_center",
                "warehouse",
                "receiving_center",
                "warehouse",
            ],
            "SHP_SITE_ID": [
                "BR-SP-CAJ",
                "BR-SP-ARA",
                "BR-SP-FRA",
                "BR-SP-GRU",
                "BR-MG-BET",
                "BR-MG-EXT",
                "BR-SC-GOV",
                "BR-BA-LAU",
            ],
        }
        return pd.DataFrame(facilities_data)

    def generate_sellers_data(self):
        """Gera dados dos sellers"""
        sellers = []

        # Calcular quantidade exata por estado
        sellers_count = {}
        remaining_sellers = self.total_sellers
        states = list(self.state_distribution.keys())

        for i, state in enumerate(states):
            if i == len(states) - 1:  # último estado fica com o restante
                count = remaining_sellers
            else:
                count = int(self.total_sellers * self.state_distribution[state])
                remaining_sellers -= count

            sellers_count[state] = count

        # Gerar sellers
        seller_id = 1
        for state, count in sellers_count.items():
            for i in range(count):
                sellers.append(
                    {"CUS_CUST_ID": f"S{seller_id:03d}", "ADD_STATE_NAME_SHP": state}
                )
                seller_id += 1

        return pd.DataFrame(sellers)

    def calculate_arrival_time(self, appointment_time, status):
        """Calcula tempo de chegada COM DISTRIBUIÇÃO REALISTA"""
        appointment_dt = datetime.strptime(appointment_time, "%Y-%m-%d %H:%M:%S")

        if status == "delivered":
            # Distribuição realista para delivered:
            # 50% no prazo (até 15min), 30% pequeno atraso (15-60min), 20% atraso (60-180min)
            rand = random.random()
            if rand < 0.50:  # 50% no prazo
                variation = random.randint(-15, 15)
            elif rand < 0.80:  # 30% pequeno atraso
                variation = random.randint(15, 60)
            else:  # 20% atraso
                variation = random.randint(60, 180)

        elif status == "delayed":
            # Atraso significativo (120-300min)
            variation = random.randint(120, 300)
        elif status == "in_progress":
            # Chegada próxima, mas ainda em processamento
            variation = random.randint(-15, 30)
        else:  # cancelled, scheduled
            variation = random.randint(-15, 15)

        arrival_dt = appointment_dt + timedelta(minutes=variation)
        return arrival_dt.strftime("%Y-%m-%d %H:%M:%S")

    def generate_operations_data(self):
        """Gera dados das operações inbound com distribuição realista"""
        operations = []

        # Gerar sellers por estado para mapeamento
        sellers_df = self.generate_sellers_data()
        state_sellers = (
            sellers_df.groupby("ADD_STATE_NAME_SHP")["CUS_CUST_ID"]
            .apply(list)
            .to_dict()
        )

        # Mapeamento CD -> Estado
        warehouse_state = {
            "WH01": "São Paulo",
            "WH02": "São Paulo",
            "WH03": "São Paulo",
            "WH04": "São Paulo",
            "WH05": "Minas Gerais",
            "WH06": "Minas Gerais",
            "WH07": "Santa Catarina",
            "WH08": "Bahia",
        }

        operation_id = 1

        for _ in range(self.total_operations):
            # Selecionar status baseado na distribuição
            status = np.random.choice(
                list(self.status_distribution.keys()),
                p=list(self.status_distribution.values()),
            )

            # Selecionar CD
            warehouse = np.random.choice(
                list(self.warehouse_distribution.keys()),
                p=list(self.warehouse_distribution.values()),
            )

            # Selecionar seller do estado correspondente ao CD
            state = warehouse_state[warehouse]
            seller = random.choice(state_sellers[state])

            # Gerar data e hora do agendamento
            appointment_date = random.choice(self.date_range)
            time_slot = random.choice(self.time_slots)
            appointment_time = f"{appointment_date.strftime('%Y-%m-%d')} {time_slot}:00"

            # Calcular tempo de chegada
            arrival_time = self.calculate_arrival_time(appointment_time, status)

            operations.append(
                {
                    "CUS_CUST_ID": seller,
                    "WAREHOUSE_ID": warehouse,
                    "INVENTORY_ID": f"INV{operation_id:04d}",
                    "MIN_APPOINTMENT_DATETIME_TZ": appointment_time,
                    "MIN_ARRIVAL_DATETIME_TZ": arrival_time,
                    "LAST_INB_STATUS": status,
                }
            )

            operation_id += 1

        return pd.DataFrame(operations)

    def generate_all_data(self):
        """Gera todos os datasets"""
        print("🔄 Gerando dados de facilities...")
        facilities_df = self.generate_facilities_data()

        print("🔄 Gerando dados de sellers...")
        sellers_df = self.generate_sellers_data()

        print("🔄 Gerando dados de operações (isso pode levar alguns segundos)...")
        operations_df = self.generate_operations_data()

        return facilities_df, sellers_df, operations_df

    def save_to_csv(self, facilities_df, sellers_df, operations_df):
        """Salva todos os datasets em arquivos CSV"""
        facilities_df.to_csv("lk_shp_facilities.csv", index=False)
        sellers_df.to_csv("lk_sf_commercial_sellers_data.csv", index=False)
        operations_df.to_csv("bt_fbm_inbound_operations_agg.csv", index=False)

        print("✅ Arquivos CSV gerados com sucesso!")
        print(f"📊 Facilities: {len(facilities_df)} registros")
        print(f"📊 Sellers: {len(sellers_df)} registros")
        print(f"📊 Operations: {len(operations_df)} registros")

        # Estatísticas detalhadas
        print("\n📈 ESTATÍSTICAS DETALHADAS:")
        print(
            f"Sellers por estado:\n{sellers_df['ADD_STATE_NAME_SHP'].value_counts().sort_index()}"
        )
        print(f"\nOperações por estado:")
        state_ops = (
            operations_df.merge(sellers_df, on="CUS_CUST_ID")["ADD_STATE_NAME_SHP"]
            .value_counts()
            .sort_index()
        )
        print(state_ops)
        print(
            f"\nOperações por warehouse:\n{operations_df['WAREHOUSE_ID'].value_counts().sort_index()}"
        )
        print(
            f"\nOperações por status:\n{operations_df['LAST_INB_STATUS'].value_counts()}"
        )


# Execução principal
if __name__ == "__main__":
    print("🚀 Iniciando geração de dados sintéticos para Inbound Optimization...")
    print("📊 Distribuição REALISTA: Sellers seguem mesma lógica das operações")

    generator = InboundDataGenerator(seed=42)
    facilities, sellers, operations = generator.generate_all_data()
    generator.save_to_csv(facilities, sellers, operations)
