package endpoints

import (
	"bytes"
	"net/http"

	"argc.in/graveldb/pkg/namespace"
	"github.com/gorilla/mux"
)

func NewCreateNamespaceHandler(svc namespace.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := mux.Vars(r)["namespace"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		if err := svc.CreateNamespace(ns); err != nil {
			writeErrorResponse(w, err, 0)
			return
		}

		w.WriteHeader(http.StatusCreated)
	})
}

func NewDeleteNamespaceHandler(svc namespace.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := mux.Vars(r)["namespace"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		if err := svc.DeleteNamespace(ns); err != nil {
			writeErrorResponse(w, err, 0)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})
}

func NewGetKeyHandler(svc namespace.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		ns := mux.Vars(r)["namespace"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		key := mux.Vars(r)["key"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		value, err := svc.GetKey(ns, key)
		if err != nil {
			writeErrorResponse(w, err, 0)
			return
		}

		w.WriteHeader(http.StatusAccepted)

		// nolint:errcheck
		w.Write(value)
	})
}

func NewSetKeyHandler(svc namespace.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		ns := mux.Vars(r)["namespace"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		key := mux.Vars(r)["key"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		var buf bytes.Buffer
		_, err := buf.ReadFrom(r.Body)
		if err != nil {
			writeErrorResponse(w, err, 0)
			return
		}

		if err := svc.SetKey(ns, key, buf.Bytes()); err != nil {
			writeErrorResponse(w, err, 0)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})
}

func NewDeleteKeyHandler(svc namespace.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := mux.Vars(r)["namespace"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		key := mux.Vars(r)["key"]
		if ns == "" {
			writeErrorResponse(w, nil, http.StatusBadRequest)
			return
		}

		if err := svc.DeleteKey(ns, key); err != nil {
			writeErrorResponse(w, err, 0)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})
}

func RegisterRoutes(r *mux.Router, svc namespace.Service) {
	r.Methods(http.MethodPost).
		Path("/api/namespaces/{namespace}").
		Handler(NewCreateNamespaceHandler(svc))
	r.Methods(http.MethodDelete).
		Path("/api/namespaces/{namespace}").
		Handler(NewDeleteNamespaceHandler(svc))
	r.Methods(http.MethodGet).
		Path("/api/namespaces/{namespace}/keys/{key}").
		Handler(NewGetKeyHandler(svc))
	r.Methods(http.MethodPut).
		Path("/api/namespaces/{namespace}/keys/{key}").
		Handler(NewSetKeyHandler(svc))
	r.Methods(http.MethodDelete).
		Path("/api/namespaces/{namespace}/keys/{key}").
		Handler(NewDeleteKeyHandler(svc))
}

func writeErrorResponse(w http.ResponseWriter, err error, code int) {
	switch err {
	case namespace.ErrNamespaceAlreadyExists:
		w.WriteHeader(http.StatusConflict)
	case namespace.ErrNamespaceDeleting:
		w.WriteHeader(http.StatusBadRequest)
	case namespace.ErrNamespaceNotFound:
		w.WriteHeader(http.StatusNotFound)
	case namespace.ErrKeyNotFound:
		w.WriteHeader(http.StatusNotFound)
	default:
		if code != 0 {
			w.WriteHeader(code)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}
