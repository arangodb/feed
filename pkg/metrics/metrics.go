package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	DatabasesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_databases_dropped_total",
		Help: "The total number of databases dropped.",
	})
	CollectionsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_collections_created_total",
		Help: "The total number of collections created.",
	})
	CollectionsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_collections_dropped_total",
		Help: "The total number of collections dropped.",
	})
	CollectionsTruncated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_collections_truncated_total",
		Help: "The total number of collections truncated.",
	})
	IndexesCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_indexes_created_total",
		Help: "The total number of indexes created.",
	})
	IndexesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_indexes_dropped_total",
		Help: "The total number of indexes dropped.",
	})
	GraphsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_graphs_created_total",
		Help: "The total number of graphs created.",
	})
	GraphsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_graphs_dropped_total",
		Help: "The total number of graphs dropped.",
	})
	DocumentsInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_documents_inserted_total",
		Help: "The total number of documents inserted.",
	})
	DocumentsReplaced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_documents_replaced_total",
		Help: "The total number of documents replaced.",
	})
	DocumentsUpdated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_documents_update_total",
		Help: "The total number of documents updated.",
	})
	DocumentsRead = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_documents_read_total",
		Help: "The total number of documents read.",
	})
	BatchesInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_batches_inserted_total",
		Help: "The total number of batches inserted.",
	})
	BatchesReplaced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_batches_replaced_total",
		Help: "The total number of batches replaced.",
	})
	BatchesUpdated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_batches_update_total",
		Help: "The total number of batches updated.",
	})
	BatchesRead = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_batches_read_total",
		Help: "The total number of batches read.",
	})
	GraphTraversals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_graph_traversals_total",
		Help: "The total number of graph traversals run.",
	})
	QueriesReplayed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_replayed_aql_queries_total",
		Help: "The total number of AQL queries replayed.",
	})
	QueriesReplayedErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "feed_replayed_aql_queries_errors_total",
		Help: "The total number of errors occurred in replayed AQL queries.",
	})
)
