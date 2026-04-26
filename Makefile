.PHONY: up down seed demo validate clean logs status

# ── Stack lifecycle ─────────────────────────────────────────────

up:
	docker compose up -d --build
	@echo ""
	@echo "Stack starting... Services will be ready in ~2-3 minutes."
	@echo "  OpenMetadata UI:  http://localhost:8585  (admin / admin)"
	@echo "  Airflow UI:       http://localhost:8080  (admin / admin)"
	@echo "  Dashboard:        http://localhost:8501"
	@echo "  MCP Server:       http://localhost:8000/mcp"
	@echo ""
	@echo "Run 'make seed' once the stack is healthy."

down:
	docker compose down

clean:
	docker compose down -v --remove-orphans
	@echo "Volumes removed. Fresh start on next 'make up'."

# ── Seed & Demo ─────────────────────────────────────────────────

seed:
	@echo "Seeding OpenMetadata with sample data and syncing contracts..."
	docker compose exec contract-sync python /app/sync.py || \
		python seed/setup.py
	@echo "Seed complete."

demo: seed
	@echo ""
	@echo "Simulating a contract violation..."
	python seed/demo_violation.py
	@echo ""
	@echo "Demo complete. Check:"
	@echo "  - Dashboard Violations page: http://localhost:8501"
	@echo "  - MCP tool: detect_drift('seller-transactions')"

# ── Local validation (no Docker required) ───────────────────────

validate:
ifndef CONTRACT
	@echo "Usage: make validate CONTRACT=contracts/seller-transactions.yaml"
	@exit 1
endif
	@echo "Validating $(CONTRACT)..."
	python -c "\
	import sys; sys.path.insert(0, 'contract_engine'); \
	from validator import validate_against_schema, load_contract; \
	errors = validate_against_schema('$(CONTRACT)'); \
	c = load_contract('$(CONTRACT)'); \
	print(f'Contract: {c.data_product} v{c.version} ({c.domain})'); \
	print(f'Schema errors: {len(errors)}'); \
	[print(f'  - {e}') for e in errors]; \
	print('PASS' if not errors else 'FAIL'); \
	sys.exit(1 if errors else 0)"

# ── Utilities ───────────────────────────────────────────────────

logs:
	docker compose logs -f --tail=50

status:
	@echo "=== Container Status ==="
	@docker compose ps
	@echo ""
	@echo "=== OpenMetadata Health ==="
	@curl -sf http://localhost:8585/api/v1/system/version 2>/dev/null && echo " OK" || echo " NOT READY"
