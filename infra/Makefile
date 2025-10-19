### Main Makefile - gdp-elt-pipeline
## Use Bash
SHELL := /bin/bash

## Include
# Generic Terraform helpers
include ./scripts/Makefile.util
# Dev helpers for LocalStack
include ./scripts/Makefile.util.dev

## Variables
# Terraform directories
TERRAFORM_DIR := $(CURDIR)/terraform/env
# DEV variables
DEV_DIR := $(TERRAFORM_DIR)/dev
# PROD variables
PROD_DIR := $(TERRAFORM_DIR)/prod

# ----------------------------
# DEV Targets
# ----------------------------
dev-up:
	$(call export_tf_vars,dev)
	$(call start_local_stack)
	$(call tf,dev,init)
	$(call tf,dev,plan)
	$(call tf,dev,apply)

dev-down:
	$(call tf,dev,destroy)
	$(call stop_local_stack)

# ----------------------------
# PROD Targets
# ----------------------------
prod-up:
	$(call export_tf_vars,dev)
	$(call tf,prod,init)
	$(call tf,prod,plan)
	$(call tf,prod,apply)

prod-down:
	$(call tf,prod,destroy)

# ----------------------------
# Help
# ----------------------------
help:
	@echo "Available targets:"
	@echo "  dev-up      # start LocalStack, init, plan, apply dev environment"
	@echo "  dev-down    # destroy dev environment and stop LocalStack"
	@echo "  prod-up     # init, plan, apply prod environment"
	@echo "  prod-down   # destroy prod environment"