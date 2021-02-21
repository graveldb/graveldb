package namespace_test

import (
	"context"
	"sync"
	"testing"

	logtesting "github.com/go-logr/logr/testing"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
	"github.com/pkg/errors"

	"argc.in/graveldb/pkg/kv"
	kvmock "argc.in/graveldb/pkg/kv/mock"
	"argc.in/graveldb/pkg/namespace"
	"argc.in/graveldb/pkg/tock"
)

func TestIsActive(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("active namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		has, err := svc.IsActive("test_ns")
		is.NoErr(err) // check namespace without error
		is.True(has)  // namespace does exist
	})
	t.Run("non-existing namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, kv.ErrKeyNotFound)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		has, err := svc.IsActive("test_ns")
		is.NoErr(err) // check namespace without error
		is.True(!has) // namespace does not exist
	})
	t.Run("deleted namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceDeletingValue, nil)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		has, err := svc.IsActive("test_ns")
		is.NoErr(err) // check namespace without error
		is.True(!has) // namespace is marked for deletion
	})
	t.Run("error getting namespace key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, testErr)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		_, err := svc.IsActive("test_ns")
		is.True(errors.Is(err, testErr)) // error should be propagated!
	})
}

func TestCreateNamespace(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("create new namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, kv.ErrKeyNotFound),
			db.EXPECT().Set([]byte(namespace.MetaPrefix+".test_ns"), namespace.NamespaceExistsValue).Return(nil),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.CreateNamespace("test_ns")
		is.NoErr(err) // create namespace without error
	})
	t.Run("re-create existing namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.CreateNamespace("test_ns")
		is.True(errors.Is(err, namespace.ErrNamespaceAlreadyExists)) // error should be Namespace Already Exists!
	})
	t.Run("re-create namespace marked for deletion", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceDeletingValue, nil),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.CreateNamespace("test_ns")
		is.True(errors.Is(err, namespace.ErrNamespaceDeleting)) // error should be Namespace Is Deleting!
	})
	t.Run("error getting namespace key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, testErr),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.CreateNamespace("test_ns")
		is.True(errors.Is(err, testErr)) // error should be propagated!
	})
	t.Run("error setting namespace key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, kv.ErrKeyNotFound),
			db.EXPECT().Set([]byte(namespace.MetaPrefix+".test_ns"), namespace.NamespaceExistsValue).Return(testErr),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.CreateNamespace("test_ns")
		is.True(errors.Is(err, testErr)) // error should be propagated!
	})
}

func TestDeleteNamespace(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("delete namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Set([]byte(namespace.MetaPrefix+".test_ns"), namespace.NamespaceDeletingValue).Return(nil),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteNamespace("test_ns")
		is.NoErr(err) // delete namespace without error
	})
	t.Run("re-delete namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceDeletingValue, nil)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteNamespace("test_ns")
		is.True(errors.Is(err, namespace.ErrNamespaceDeleting)) // errors should be Namespace Is Deleting!
	})
	t.Run("delete non-existing namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, kv.ErrKeyNotFound)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteNamespace("test_ns")
		is.True(errors.Is(err, namespace.ErrNamespaceNotFound)) // errors should be Namespace Not Found!
	})
	t.Run("error getting namespace key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, testErr),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteNamespace("test_ns")
		is.True(errors.Is(err, testErr)) // error should be propagated!
	})
	t.Run("error setting namespace key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Set([]byte(namespace.MetaPrefix+".test_ns"), namespace.NamespaceDeletingValue).Return(testErr),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteNamespace("test_ns")
		is.True(errors.Is(err, testErr)) // error should be propagated!
	})
}

func TestGetKey(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("get key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Get([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return([]byte("test_value"), nil),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		value, err := svc.GetKey("test_ns", "test_key")
		is.NoErr(err)                         // get key without error
		is.Equal(value, []byte("test_value")) // return value must be equal to mock value
	})
	t.Run("get non-existing key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Get([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return(nil, kv.ErrKeyNotFound),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		value, err := svc.GetKey("test_ns", "test_key")
		is.True(errors.Is(err, kv.ErrKeyNotFound)) // error should be Key Not Found!
		is.Equal(nil, value)                       // value should be nil
	})
	t.Run("get key from non-existing namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, kv.ErrKeyNotFound)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		value, err := svc.GetKey("test_ns", "test_key")
		is.True(errors.Is(err, namespace.ErrNamespaceNotFound)) // error should be Namespace Not Found!
		is.Equal(nil, value)                                    // value should be nil
	})
	t.Run("get key from namespace marked for deletion", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceDeletingValue, nil)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		value, err := svc.GetKey("test_ns", "test_key")
		is.True(errors.Is(err, namespace.ErrNamespaceNotFound)) // error should be Namespace Not Found!
		is.Equal(nil, value)                                    // value should be nil
	})
	t.Run("error checking active namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, testErr)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		value, err := svc.GetKey("test_ns", "test_key")
		is.True(errors.Is(err, testErr)) // error should be propagated
		is.Equal(nil, value)             // value should be nil
	})
}

func TestSetKey(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("set key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Set([]byte(namespace.NamespacePrefix+".test_ns.test_key"), []byte("test_value")).Return(nil),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.SetKey("test_ns", "test_key", []byte("test_value"))
		is.NoErr(err) // set key without error
	})
	t.Run("error setting key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Set([]byte(namespace.NamespacePrefix+".test_ns.test_key"), []byte("test_value")).Return(testErr),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.SetKey("test_ns", "test_key", []byte("test_value"))
		is.True(errors.Is(err, testErr)) // error should be propagated
	})
	t.Run("set key from non-existing namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, kv.ErrKeyNotFound)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.SetKey("test_ns", "test_key", nil)
		is.True(errors.Is(err, namespace.ErrNamespaceNotFound)) // error should be Namespace Not Found!
	})
	t.Run("set key from namespace marked for deletion", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceDeletingValue, nil)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.SetKey("test_ns", "test_key", nil)
		is.True(errors.Is(err, namespace.ErrNamespaceNotFound)) // error should be Namespace Not Found!
	})
	t.Run("error checking active namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, testErr)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.SetKey("test_ns", "test_key", nil)
		is.True(errors.Is(err, testErr)) // error should be propagated
	})
}

func TestDeleteKey(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("delete key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Delete([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return(nil),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteKey("test_ns", "test_key")
		is.NoErr(err) // delete key without error
	})
	t.Run("error deleting key", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		gomock.InOrder(
			db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceExistsValue, nil),
			db.EXPECT().Delete([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return(testErr),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteKey("test_ns", "test_key")
		is.True(errors.Is(err, testErr)) // error should be propagated
	})
	t.Run("delete key from non-existing namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, kv.ErrKeyNotFound)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteKey("test_ns", "test_key")
		is.True(errors.Is(err, namespace.ErrNamespaceNotFound)) // error should be Namespace Not Found!
	})
	t.Run("delete key from namespace marked for deletion", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(namespace.NamespaceDeletingValue, nil)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteKey("test_ns", "test_key")
		is.True(errors.Is(err, namespace.ErrNamespaceNotFound)) // error should be Namespace Not Found!
	})
	t.Run("error checking active namespace", func(t *testing.T) {
		db := kvmock.NewMockService(ctrl)
		testErr := errors.New("test-error")
		db.EXPECT().Get([]byte(namespace.MetaPrefix+".test_ns")).Return(nil, testErr)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))

		err := svc.DeleteKey("test_ns", "test_key")
		is.True(errors.Is(err, testErr)) // error should be propagated
	})
}

func TestGarbageCollection(t *testing.T) {
	// is := is.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("garbage-collect single deleted namespace", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())
		db := kvmock.NewMockService(ctrl)
		iter := kvmock.NewMockIterator(ctrl)
		gomock.InOrder(
			db.EXPECT().NewIterator([]byte(namespace.MetaPrefix+"."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Value().Return(namespace.NamespaceDeletingValue),
			iter.EXPECT().Key().Return([]byte(namespace.MetaPrefix+".test_ns")),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(false),
			iter.EXPECT().Close(),
			db.EXPECT().RWBatch(gomock.Any()).DoAndReturn(func(f kv.BatchReadWriterFunc) error {
				return f(db)
			}),
			db.EXPECT().NewIterator([]byte(namespace.NamespacePrefix+".test_ns."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Key().Return([]byte(namespace.NamespacePrefix+".test_ns.test_key")),
			db.EXPECT().Delete([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return(nil),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(false),
			db.EXPECT().Delete([]byte(namespace.MetaPrefix+".test_ns")).Return(nil),
			iter.EXPECT().Close(),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))
		ticker := tock.NewTestTick()
		wg.Add(1)
		go func() {
			svc.GarbageCollection(ctx, ticker)
			ticker.Stop()
			wg.Done()
		}()
		ticker.Tick()
		cancel()
		wg.Wait()
	})
	t.Run("garbage-collect one deleted namespace among other namespaces", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())
		db := kvmock.NewMockService(ctrl)
		iter := kvmock.NewMockIterator(ctrl)
		gomock.InOrder(
			db.EXPECT().NewIterator([]byte(namespace.MetaPrefix+"."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Value().Return(namespace.NamespaceDeletingValue),
			iter.EXPECT().Key().Return([]byte(namespace.MetaPrefix+".test_ns")),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Value().Return(namespace.NamespaceExistsValue),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(false),
			iter.EXPECT().Close(),
			db.EXPECT().RWBatch(gomock.Any()).DoAndReturn(func(f kv.BatchReadWriterFunc) error {
				return f(db)
			}),
			db.EXPECT().NewIterator([]byte(namespace.NamespacePrefix+".test_ns."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Key().Return([]byte(namespace.NamespacePrefix+".test_ns.test_key")),
			db.EXPECT().Delete([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return(nil),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(false),
			db.EXPECT().Delete([]byte(namespace.MetaPrefix+".test_ns")).Return(nil),
			iter.EXPECT().Close(),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))
		ticker := tock.NewTestTick()
		wg.Add(1)
		go func() {
			svc.GarbageCollection(ctx, ticker)
			ticker.Stop()
			wg.Done()
		}()
		ticker.Tick()
		cancel()
		wg.Wait()
	})
	t.Run("error while deleting namespace key", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())
		testErr := errors.New("test-err")
		db := kvmock.NewMockService(ctrl)
		iter := kvmock.NewMockIterator(ctrl)
		gomock.InOrder(
			db.EXPECT().NewIterator([]byte(namespace.MetaPrefix+"."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Value().Return(namespace.NamespaceDeletingValue),
			iter.EXPECT().Key().Return([]byte(namespace.MetaPrefix+".test_ns")),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(false),
			iter.EXPECT().Close(),
			db.EXPECT().RWBatch(gomock.Any()).DoAndReturn(func(f kv.BatchReadWriterFunc) error {
				return f(db)
			}),
			db.EXPECT().NewIterator([]byte(namespace.NamespacePrefix+".test_ns."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Key().Return([]byte(namespace.NamespacePrefix+".test_ns.test_key")),
			db.EXPECT().Delete([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return(testErr),
			iter.EXPECT().Close(),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))
		ticker := tock.NewTestTick()
		wg.Add(1)
		go func() {
			svc.GarbageCollection(ctx, ticker)
			ticker.Stop()
			wg.Done()
		}()
		ticker.Tick()
		cancel()
		wg.Wait()
	})
	t.Run("error while deleting meta key", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())
		testErr := errors.New("test-error")
		db := kvmock.NewMockService(ctrl)
		iter := kvmock.NewMockIterator(ctrl)
		gomock.InOrder(
			db.EXPECT().NewIterator([]byte(namespace.MetaPrefix+"."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Value().Return(namespace.NamespaceDeletingValue),
			iter.EXPECT().Key().Return([]byte(namespace.MetaPrefix+".test_ns")),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(false),
			iter.EXPECT().Close(),
			db.EXPECT().RWBatch(gomock.Any()).DoAndReturn(func(f kv.BatchReadWriterFunc) error {
				return f(db)
			}),
			db.EXPECT().NewIterator([]byte(namespace.NamespacePrefix+".test_ns."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(true),
			iter.EXPECT().Key().Return([]byte(namespace.NamespacePrefix+".test_ns.test_key")),
			db.EXPECT().Delete([]byte(namespace.NamespacePrefix+".test_ns.test_key")).Return(nil),
			iter.EXPECT().Next(),
			iter.EXPECT().Valid().Return(false),
			db.EXPECT().Delete([]byte(namespace.MetaPrefix+".test_ns")).Return(testErr),
			iter.EXPECT().Close(),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))
		ticker := tock.NewTestTick()
		wg.Add(1)
		go func() {
			svc.GarbageCollection(ctx, ticker)
			ticker.Stop()
			wg.Done()
		}()
		ticker.Tick()
		cancel()
		wg.Wait()
	})
	t.Run("no deleted namespace", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())
		db := kvmock.NewMockService(ctrl)
		iter := kvmock.NewMockIterator(ctrl)
		gomock.InOrder(
			db.EXPECT().NewIterator([]byte(namespace.MetaPrefix+"."), nil).Return(iter),
			iter.EXPECT().First(),
			iter.EXPECT().Valid().Return(false),
			iter.EXPECT().Close(),
		)
		log := logtesting.TestLogger{T: t}
		svc := namespace.NewService(db, namespace.WithLogger(log))
		ticker := tock.NewTestTick()
		wg.Add(1)
		go func() {
			svc.GarbageCollection(ctx, ticker)
			ticker.Stop()
			wg.Done()
		}()
		ticker.Tick()
		cancel()
		wg.Wait()
	})
}
