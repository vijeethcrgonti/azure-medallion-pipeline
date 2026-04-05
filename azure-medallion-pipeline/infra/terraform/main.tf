terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-tfstate"
    storage_account_name = "medalliontfstate"
    container_name       = "tfstate"
    key                  = "medallion.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# ── Resource Group ─────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = "rg-medallion-${var.environment}"
  location = var.location
  tags     = local.common_tags
}

# ── ADLS Gen2 ──────────────────────────────────────────────────────────────────

resource "azurerm_storage_account" "adls" {
  name                     = "adlsmedallion${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical namespace = ADLS Gen2

  tags = local.common_tags
}

resource "azurerm_storage_container" "medallion" {
  name                  = "medallion"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# ── Azure Databricks Workspace ─────────────────────────────────────────────────

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-medallion-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "standard"
  tags                = local.common_tags
}

# ── Azure Data Factory ─────────────────────────────────────────────────────────

resource "azurerm_data_factory" "main" {
  name                = "adf-medallion-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

# ── Azure Synapse Analytics ────────────────────────────────────────────────────

resource "azurerm_synapse_workspace" "main" {
  name                                 = "synapse-medallion-${var.environment}"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_container.medallion.id
  sql_administrator_login              = var.synapse_admin_login
  sql_administrator_login_password     = var.synapse_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

# ── Key Vault ──────────────────────────────────────────────────────────────────

resource "azurerm_key_vault" "main" {
  name                = "kv-medallion-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku_name            = "standard"
  tenant_id           = data.azurerm_client_config.current.tenant_id

  tags = local.common_tags
}

data "azurerm_client_config" "current" {}

locals {
  common_tags = {
    project     = "azure-medallion-pipeline"
    environment = var.environment
    managed_by  = "terraform"
  }
}
