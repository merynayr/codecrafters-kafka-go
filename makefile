# Локальный запуск тестера CodeCrafters для Kafka.
#   make stage S=nv3   — только этап nv3
#   make upto  S=wa6   — все этапы от первого до wa6 включительно
#   make all           — все этапы
TESTER_DIR ?= ../kafka-tester
TESTER     := $(TESTER_DIR)/dist/main.out
HASH       := \#

STAGES := vi6 nv3 wa6 nc5 pv1 \
          nh4 sk0 \
          yk1 vt6 ea7 ku4 wq2 \
          gs0 dh6 hn6 cm4 eg2 fd8 \
          xz1 zf2 gg1 ls8 yd8 ct4 ov0

# Собирает JSON со списком этапов для CODECRAFTERS_TEST_CASES_JSON
cases_json = [$(shell for s in $(1); do \
  u=$$(echo $$s | tr '[:lower:]' '[:upper:]'); \
  printf '{"slug":"%s","tester_log_prefix":"tester::$(HASH)%s","title":"Stage %s"},' $$s $$u $$u; \
  done | sed 's/,$$//')]

run = CODECRAFTERS_REPOSITORY_DIR=$(CURDIR) \
      CODECRAFTERS_TEST_CASES_JSON='$(call cases_json,$(1))' \
      $(TESTER)

.PHONY: stage upto all tester check-s

stage: $(TESTER) check-s
	@$(call run,$(S))

upto: $(TESTER) check-s
	@$(call run,$(shell echo $(STAGES) | tr ' ' '\n' | sed '/^$(S)$$/q'))

all: $(TESTER)
	@$(call run,$(STAGES))

check-s:
	@test -n "$(S)" || { echo "Укажи этап: make $(MAKECMDGOALS) S=nv3"; exit 1; }
	@echo " $(STAGES) " | grep -q " $(S) " || { echo "Неизвестный этап: $(S)"; exit 1; }

# Пересобрать тестер (например, после git pull в kafka-tester)
tester:
	$(MAKE) -C $(TESTER_DIR) build

$(TESTER):
	$(MAKE) -C $(TESTER_DIR) build