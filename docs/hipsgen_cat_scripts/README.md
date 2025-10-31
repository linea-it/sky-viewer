# Configuração de Ambiente Conda e Kernel Python no Open OnDemand (LIneA) para os Scripts Relacionados ao Hipsgen-cat

Autor: Luigi Silva
Última verificação: 12/02/2024

## Acessando o Open OnDemand
Para acessar a plataforma Open OnDemand, siga as instruções abaixo:

1. Acesse o site: [ondemand.linea.org.br](https://ondemand.linea.org.br)
2. Na página inicial do Open OnDemand, clique em: **Clusters -> LIneA Shell Access**

---

## Configurando o Ambiente no LIneA Shell Access

### 1. Vá para seu diretório `$SCRATCH`, instale e carregue o Miniconda
Execute os seguintes comandos no terminal:

```bash
cd $SCRATCH
curl -L -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh -p $SCRATCH/miniconda
source miniconda/bin/activate
conda deactivate  # Necessário para desativar o ambiente "base"
```

### 2. Crie o ambiente java_env utilizando o arquivo java_env.yaml
Use o arquivo java_env.yaml para criar o ambiente. Certifique-se de que o arquivo está no diretório em que você está trabalhando.

Como criar o ambiente:

```bash
conda create -p $SCRATCH/java_env --file java_env.yaml
```

Como ativar o ambiente:

```bash
conda activate $SCRATCH/java_env
```

### 3. Configure o JUPYTER_PATH e crie o Kernel no Jupyter Notebook
Configure o caminho obrigatório para o Jupyter e instale o kernel:

```bash
JUPYTER_PATH=$SCRATCH/.local
echo $JUPYTER_PATH
python -m ipykernel install --prefix=$JUPYTER_PATH --name 'java_env'
```

### Observação
Após seguir os passos acima, o kernel java_env estará disponível para uso no Jupyter Notebook na plataforma Open OnDemand.

## Executando os notebooks e scripts

### 1. Notebooks
Para executar os notebooks, basta selecionar o kernel java_env e rodar as células.

### 2. Scripts

Antes de executar os scripts, todos devem ser movidos para a pasta hipsgen_cat.

```bash
mkdir $SCRATCH/hipsgen_cat
mv /path/to/scripts/* $SCRATCH/hipsgen_cat
cd $SCRATCH/hipsgen_cat
```

Além disso, deve-se criar a seguinte pasta

```bash
mkdir $SCRATCH/hipsgen_cat/logs_of_submission/
```

Os caminhos dos inputs nos scripts ```hipsgen_csv_input.py``` e ```hipsgen_fits_input.py``` devem ser ajustados antes de serem executados.

Os scripts podem, então, ser executados no terminal do ambiente HPC do LIneA com

```bash
sbatch nome_do_script.sbatch
```

## Referências:
https://docs.linea.org.br/processamento/uso/openondemand.html