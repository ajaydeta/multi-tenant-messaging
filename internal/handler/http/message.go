package http

import (
	"net/http"
	"strconv"

	"multi-tenant-messaging/internal/service"

	"github.com/labstack/echo/v4"
)

type MessageHandler struct {
	service service.MessageService
}

func NewMessageHandler(s service.MessageService) *MessageHandler {
	return &MessageHandler{service: s}
}

// GetMessages godoc
// @Summary Get messages with cursor pagination
// @Description Retrieves a list of messages using cursor-based pagination.
// @Tags messages
// @Produce  json
// @Param   cursor    query     string  false  "Cursor for next page"
// @Param   limit     query     int     false  "Limit per page"
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} echo.HTTPError
// @Router /messages [get]
func (h *MessageHandler) GetMessages(c echo.Context) error {
	cursor := c.QueryParam("cursor")
	limitStr := c.QueryParam("limit")
	limit, _ := strconv.Atoi(limitStr)

	messages, nextCursor, err := h.service.GetMessages(c.Request().Context(), cursor, limit)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	response := map[string]interface{}{
		"data":        messages,
		"next_cursor": nextCursor,
	}

	return c.JSON(http.StatusOK, response)
}
