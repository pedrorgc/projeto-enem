import gcsfs
import json

with open('C:/Users/Ronnie/OneDrive/Documentos/Estudos/ADS/BD2/projeto-enem/EngenhariaDadosEnem-main/chave/enem-ifnmg-pedrorgc-raipb-da426803e45c.json') as f:
    cred = json.load(f)

fs = gcsfs.GCSFileSystem(token=cred)
print(fs.ls('enem-bucket-pedrorgc-raipb'))
