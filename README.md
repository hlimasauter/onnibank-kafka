# Kafka Elo Out Exporter - Helm Chart

Este repositório contém o Helm Chart para o deploy do `kafka-elo-out-exporter` e a automação de CI/CD para o **Oracle Kubernetes Engine (OKE)**.

## 🚀 1. Configuração da Pipeline (GitHub Actions)

Para que a automação (`.github/workflows/deploy.yaml`) funcione e faça o deploy no Oracle Cloud, você precisa configurar os seguintes **Secrets** no repositório.

Vá em: **Settings** > **Secrets and variables** > **Actions** > **New repository secret**.

| Nome da Secret | Descrição | Onde encontrar (Oracle OCI) |
| :--- | :--- | :--- |
| `OCI_TENANCY_OCID` | ID da sua conta (Tenancy) | Profile Menu -> Tenancy: ... |
| `OCI_USER_OCID` | ID do seu usuário | Identity -> Users -> Seu Usuário |
| `OCI_REGION` | Região do Cluster | Ex: `sa-saopaulo-1` |
| `OCI_FINGERPRINT` | Digital da API Key | Identity -> Users -> API Keys |
| `OCI_API_KEY` | Conteúdo da Chave Privada (`.pem`) | Conteúdo do arquivo baixado ao criar a API Key |
| `OKE_CLUSTER_OCID` | ID do Cluster Kubernetes | Developer Services -> Kubernetes Clusters (OKE) |

> ⚠️ **Atenção com a `OCI_API_KEY`**: Copie todo o conteúdo do arquivo `.pem`, incluindo as linhas `-----BEGIN PRIVATE KEY-----` e `-----END PRIVATE KEY-----`.

---

## 💻 2. Como executar localmente (Manual)

Se você precisa rodar o deploy a partir do seu computador pessoal, siga os passos abaixo.

### Pré-requisitos
* **Helm** instalado (v3+).
* **Kubectl** configurado e apontando para o seu cluster (verifique com `kubectl get nodes`).

### Passo 1: Validar o Template (Debug)
Antes de instalar, verifique se o Helm consegue ler os arquivos corretamente e gerar o YAML final sem erros.


# Estando na raiz do projeto
* helm template debug-release ./kafka-exporter-chart

### Passo 2: Instalar ou Atualizar (Deploy)
Este comando cria o deploy se não existir, ou atualiza se já existir.

* helm upgrade --install kafka-exporter ./kafka-exporter-chart \
*  --namespace prometheus \
*  --create-namespace

### Passo 3: Verificar Status
Confira se os pods subiram e se o service foi criado.
* kubectl get all -n prometheus -l app=kafka-elo-out-exporter

### Passo 4: Verificar Configuração de IPs (HostAliases)
Para garantir que os IPs do Kafka foram injetados corretamente no /etc/hosts do container:

## Pegue o nome do pod
* POD_NAME=$(kubectl get pods -n prometheus -l app=kafka-elo-out-exporter -o jsonpath="{.items[0].metadata.name}")

## Leia o arquivo hosts dentro do pod
* kubectl exec -it $POD_NAME -n prometheus -- cat /etc/hosts
