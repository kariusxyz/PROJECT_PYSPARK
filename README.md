# Semana 09 · PySpark Avançado

Projeto prático para VS Code usado após ~50 minutos de teoria. O laboratório foi planejado para ~150 minutos. O objetivo é diagnosticar um pipeline lento e justificar cada otimização com plano físico, Spark UI e tempo de execução.

## Cronograma sugerido
- 0–10 min: abrir projeto e conferir ambiente.
- 10–25 min: gerar o dataset.
- 25–55 min: baseline + Spark UI.
- 55–75 min: TODO 1 — filtrar antes.
- 75–95 min: TODO 2 — broadcast.
- 95–120 min: TODO 3 — agregação e saída.
- 120–135 min: data skew.
- 135–145 min: evidências e conclusão.
- 145–150 min: fechamento.

## 1. Abra corretamente
1. Extraia o ZIP inteiro.
2. VS Code → **Arquivo → Abrir Pasta** → selecione `aluno_projeto`.
3. Abra **Terminal → Novo Terminal**. Confirme que o terminal está nessa pasta.

## 2. Confira o ambiente
No Windows:
```powershell
python --version
java -version
python check_env.py
```
Se `python` não existir, tente `py` nos mesmos comandos.

Referência do pacote: **Python 3.10+**, **Java 17+** e **PySpark 4.2.0**.
Se ainda não tiver PySpark:
```powershell
python -m pip install -r requirements.txt
```

## 3. Gere o dataset
```powershell
python gerar_dados.py
```
Padrão: 1.200.000 pedidos, 80 categorias e 27 estados. O gerador cria Parquet em `data/`.

Se o computador for muito fraco, use temporariamente:
```powershell
python gerar_dados.py --linhas 300000
```
O modo reduzido é contingência; para a evidência principal, prefira 1M+ linhas quando a máquina suportar.

## 4. Faça o diagnóstico do baseline
```powershell
python pipeline_lento.py
```
O script pausa antes e depois da action. Ele imprime a URL real da **Spark UI**. Normalmente será `http://localhost:4040`, mas pode ser 4041, 4042 etc.

Registre:
- estratégia física dos joins no `explain`;
- stage com maior Shuffle Read/Write;
- existência de task muito mais lenta;
- tempo total impresso pelo script.

## 5. Otimize por etapas
```powershell
python pipeline_desafio.py
```
O arquivo é válido desde o início. Ele para de propósito em cada TODO:
- TODO 1: filtrar antes;
- TODO 2: broadcast nas dimensões pequenas;
- TODO 3: agregar, ordenar e coalesce(4).

Use o caderno do aluno para preencher cada trecho.

## 6. Veja data skew
```powershell
python skew_demo.py
```
O dataset concentra cerca de 35% dos pedidos em `customer_id=1`. Compare os tamanhos das partições.

## Erros comuns
- **No module named pyspark** → `python -m pip install -r requirements.txt`
- **Java gateway / java não encontrado** → `java -version`; configure Java 17+ e `JAVA_HOME` quando necessário.
- **data/pedidos não existe** → rode `python gerar_dados.py` antes dos pipelines.
- **4040 não abre** → use a URL impressa pelo script; outra aplicação Spark pode ter ocupado 4040.
- **Execução rápida demais / diferença pequena** → confirme 1.200.000 linhas e use a mesma máquina para comparar. Hardware e cache do SO alteram tempos absolutos.
- **Processo parece parado** → verifique se o terminal está aguardando ENTER para permitir observação da Spark UI.
