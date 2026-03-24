VERSION ?=

.PHONY: release

release:
	@if [ -z "$(VERSION)" ]; then \
		echo "VERSION is required. Example: make release VERSION=v0.1.0"; \
		exit 1; \
	fi
	@if ! printf '%s' "$(VERSION)" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$$'; then \
		echo "VERSION must match stable semver format like v0.1.0"; \
		exit 1; \
	fi
	@if git rev-parse -q --verify "refs/tags/$(VERSION)" >/dev/null; then \
		echo "Tag $(VERSION) already exists locally"; \
		exit 1; \
	fi
	@if git ls-remote --exit-code --tags origin "refs/tags/$(VERSION)" >/dev/null 2>&1; then \
		echo "Tag $(VERSION) already exists on origin"; \
		exit 1; \
	fi
	@git tag "$(VERSION)"
	@git push origin "$(VERSION)"
	@echo "Created and pushed $(VERSION)"
	@echo "Release page: https://github.com/aleda145/kavla-cli/releases/tag/$(VERSION)"
