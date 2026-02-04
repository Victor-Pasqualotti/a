import json

class TelaCriaRegras:
    def __init__(self, db_cadastro=None, db_regras=None):
        self.db_cadastro = r"bancos_dados\db_cadastro.json" if db_cadastro is None else db_cadastro
        self.db_regras   = r"bancos_dados\db_regras.json" if db_regras is None else db_regras

    def cria_regra(self):
        print("=== Tela de Criação/Edição de Regras ===")
        # Infos para cadatro da regra
        nome_regra = input("Digite o nome da regra: ").lower().strip()
        descricao  = input("Digite a descrição da regra: ").lower().strip()
        
        # Infos do datamart
        projeto          = input("A regra será associada a qual projeto? ").lower().strip()
        categoria_regras = input("A regra será associada a qual categoria de regras? ").lower().strip()

        # Resgata tabelas envolvidas no datamart (conforme cadastro)
        attrs_dm = self.resgata_infos_cadastro_dm(projeto, categoria_regras)
        tabelas_glue = attrs_dm.get("Tabelas")

        # Resgata schema dessas tabelas via catalogo do Glue. E devolve possíveis regras que o usuário pode criar
        for tbl in tabelas_glue:
            table_columns = self.resgata_infos_tabela_glue(tbl)
            regras_template = self.resgata_template_regras(table_columns)
            print(" --- POSSÍVEIS REGRAS PARA A TABELA:", tbl, "---")
            for coluna, info in regras_template.items():
                print("--------------------------------------------------")
                print("Ainda não sei como mas você vai poder escolher a coluna e o tipo de regra sobre ela aqui.")
                print(f"Coluna: {coluna} | Tipo: {info['tipo_regra']} | Possíveis parâmetros: {info['parametros']}")
                # TODO Aqui o usuário criaria as regras conforme o template apresentado
                # Ainda vamos pensar como fazer isso de forma amigável
            
            self.salva_regras_db({
                "NomeRegra": nome_regra,
                "Descricao": descricao,
                "Projeto": projeto,
                "CategoriaRegras": categoria_regras,
                "Tabela": tbl,
                "TemplateRegras": regras_template
            })

    
    def resgata_infos_cadastro_dm(self, projeto, categoria_regras):
        """
        Resgata as infos de cadastro do datamart salvo em arquivo JSON
        """
        with open(self.db_cadastro, "r") as arquivo:
            dados = json.load(arquivo)
        
        for objeto in dados.get("Objetos", list()):
            pk = objeto.get("PrimaryKey", {})
            pttk = pk.get("PartitionKey", {})

            if pttk.split('#')[1] == projeto and pttk.split('#')[2] == categoria_regras:
                attrs = objeto.get("Attributes", {})
                break
        return attrs
    
    def resgata_infos_tabela_glue(self, tabela_glue):
        """
        Resgata as infos da tabela Glue salva em arquivo JSON
        """
        path = rf"catalogo_glue\{tabela_glue.strip()}.json"
        with open(path, "r") as arquivo:
            catalogo = json.load(arquivo)
        
        return catalogo.get('columns')
    
    def resgata_template_regras(self, table_columns):
        """
        Resgata o template de regras baseado nas colunas da tabela Glue
        """
        depara = {
            "string": "esta_em,contem,comeca_com,termina_com,permitir_valores,negar_valores",
            "integer": "maior_que,menor_que,permitir_intervalo",
            "bigint": "maior_que,menor_que,permitir_intervalo",
            "double": "maior_que,menor_que,permitir_intervalo",
            "decimal(12,2)": "maior_que,menor_que,permitir_intervalo",
            "float": "maior_que,menor_que,permitir_intervalo",
            "boolean": "verdadeiro,falso",
            "date": "maior_que,menor_que,no_ano_passao,nos_ultimos_n_meses",
            "timestamp": "maior_que,menor_que,no_ano_passao,nos_ultimos_n_meses"
        }

        regras_template = dict()
        always_allow = "selecionar"
        for coluna in table_columns:
            nome_coluna = coluna.get("column_name")
            regras_template[nome_coluna] = {
                "tipo_regra": coluna.get('type'),
                "parametros": depara.get(coluna.get('type'), ",").split(",") + [always_allow],
            }
        return regras_template
    
    def salva_regras_db(self, nova_regra):
        """
        Salva a nova regra criada no banco de regras (arquivo JSON)
        """
        with open(self.db_regras, "r") as arquivo:
            dados = json.load(arquivo)
        
        dados.get("Regras", list()).append(nova_regra)

        with open(self.db_regras, "w") as arquivo:
            json.dump(dados, arquivo, indent=4)

if __name__ == "__main__":
    tela = TelaCriaRegras()
    tela.cria_regra()
# Digite o nome da regra: vendas_ytd
# Digite a descrição da regra: filtra apenas vendas que ocorreram esse ano ate o presente momento
# A regra será associada a qual projeto? padaria
# A regra será associada a qual categoria de regras? contratacao