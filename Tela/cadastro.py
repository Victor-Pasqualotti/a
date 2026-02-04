import json

class TelaCadastro:
    def __init__(self, db_cadastro=None):
        self.db_cadastro = r"bancos_dados\db_cadastro.json" if db_cadastro is None else db_cadastro

    def exibe_resgata_formulario(self):
        print("=== Tela de Cadastro ===")
        projeto          = input("Digite seu projeto: ").lower().strip()
        categoria_regras = input("Digite qual categoria de regras será atendida: ").lower().strip()
        tabelas          = input("Digite suas tabelas (datamart): ").lower().strip() # Lista de tabelas que compõem o datamart
        return projeto, categoria_regras, tabelas
    
    def salva_infos_cadastro(self, projeto, categoria_regras, tabelas):
        """
        Isso é um POST no banco de dados (aqui, um arquivo JSON)
        """
        with open(self.db_cadastro, "w") as arquivo:
            dados = {
                "Objetos":[
                    {
                        "PrimaryKey":{
                            "PartitionKey": f'dm#{projeto}#{categoria_regras}',
                            "SortKey": projeto
                        },
                        "Attributes": {
                            "Projeto": projeto,
                            "CategoriaRegras": categoria_regras,
                            "Tabelas": tabelas.split(',') # Só pro exemplo funcionar: salvando a lista de tablelas
                            # Na vida real, precisamos do database e da tabela
                        }
                }
                ]
            }
            json.dump(dados, arquivo, indent=4)    
        print("Informações de cadastro salvas com sucesso!")

if __name__ == "__main__":
    tela = TelaCadastro()
    projeto, categoria_regras, tabelas = tela.exibe_resgata_formulario()
    tela.salva_infos_cadastro(projeto, categoria_regras, tabelas)

# Digite seu projeto: Padaria
# Digite qual categoria de regras será atendida: Contratacao
# Digite suas tabelas (datamart): tbl_vendas, tbl_pedidos