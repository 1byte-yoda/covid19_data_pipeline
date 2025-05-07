FROM quay.io/jupyter/minimal-notebook:latest

RUN pip install duckdb==1.2.2 pandas==2.2.3 scipy seaborn

CMD ["start-notebook.sh", "--NotebookApp.token="]