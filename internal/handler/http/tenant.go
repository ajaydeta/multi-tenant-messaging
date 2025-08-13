package http

import (
	"net/http"

	"multi-tenant-messaging/internal/service"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type TenantHandler struct {
	service service.TenantService
}

func NewTenantHandler(s service.TenantService) *TenantHandler {
	return &TenantHandler{service: s}
}

type CreateTenantRequest struct {
	Name string `json:"name" validate:"required"`
}

// CreateTenant godoc
// @Summary Create a new tenant
// @Description Creates a new tenant, sets up its message queue and consumer.
// @Tags tenants
// @Accept  json
// @Produce  json
// @Param   tenant  body    CreateTenantRequest  true  "Tenant Info"
// @Success 201 {object} repository.Tenant
// @Router /tenants [post]
func (h *TenantHandler) CreateTenant(c echo.Context) error {
	req := new(CreateTenantRequest)
	if err := c.Bind(req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	tenant, err := h.service.CreateTenant(c.Request().Context(), req.Name)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, tenant)
}

// DeleteTenant godoc
// @Summary Delete a tenant
// @Description Deletes a tenant, its consumer, and its queue.
// @Tags tenants
// @Param   id   path      string  true  "Tenant ID"
// @Success 204 "No Content"
// @Router /tenants/{id} [delete]
func (h *TenantHandler) DeleteTenant(c echo.Context) error {
	idStr := c.Param("id")
	tenantID, err := uuid.Parse(idStr)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid tenant ID format")
	}

	if err := h.service.DeleteTenant(c.Request().Context(), tenantID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

type UpdateConcurrencyRequest struct {
	Workers int `json:"workers" validate:"required,min=1"`
}

// UpdateConcurrency godoc
// @Summary Update tenant's consumer concurrency
// @Description Updates the number of concurrent workers for a tenant's consumer.
// @Tags tenants
// @Accept  json
// @Produce  json
// @Param   id   path      string  true  "Tenant ID"
// @Param   config  body    UpdateConcurrencyRequest  true  "Concurrency Config"
// @Success 200 {object} map[string]string
// @Router /tenants/{id}/config/concurrency [put]
func (h *TenantHandler) UpdateConcurrency(c echo.Context) error {
	idStr := c.Param("id")
	tenantID, err := uuid.Parse(idStr)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid tenant ID format")
	}

	req := new(UpdateConcurrencyRequest)
	if err := c.Bind(req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	if err := h.service.UpdateConcurrency(c.Request().Context(), tenantID, req.Workers); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "Concurrency updated. Restart the service for changes to take effect."})
}
