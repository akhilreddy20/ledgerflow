.PHONY: up infra down logs build test it e2e chaos load invariants

up:          ## full stack in Docker
	docker compose up -d --build
infra:       ## only Postgres, Redis, Kafka + observability (run services with npm)
	docker compose up -d postgres redis kafka kafka-ui jaeger prometheus grafana
down:        ## stop and delete volumes
	docker compose down -v
logs:
	docker compose logs -f ledger payments notifications
build:
	npm run build
test:
	npm test
it:          ## integration tests (needs Postgres)
	docker compose exec -T postgres createdb -U ledgerflow ledger_test 2>/dev/null || true
	TEST_DATABASE_URL=postgres://ledgerflow:ledgerflow@localhost:5432/ledger_test npm run test:integration
e2e:
	npm run e2e
chaos:
	npm run e2e -- --chaos
load:
	k6 run load/transfers.js
invariants:
	npm run invariants
