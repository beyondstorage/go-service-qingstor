package qingstor

import (
	"context"

	"github.com/qingstor/qingstor-sdk-go/v4/service"

	ps "github.com/aos-dev/go-storage/v2/pairs"
	typ "github.com/aos-dev/go-storage/v2/types"
)

func (s *Service) create(ctx context.Context, name string, opt *pairServiceCreate) (store typ.Storager, err error) {
	// ServicePairCreate requires location, so we don't need to add location into pairs
	pairs := append(opt.pairs, ps.WithName(name))

	st, err := s.newStorage(pairs...)
	if err != nil {
		return
	}

	_, err = st.bucket.PutWithContext(ctx)
	if err != nil {
		return
	}
	return st, nil
}
func (s *Service) delete(ctx context.Context, name string, opt *pairServiceDelete) (err error) {
	pairs := append(opt.pairs, ps.WithName(name))

	store, err := s.newStorage(pairs...)
	if err != nil {
		return
	}
	_, err = store.bucket.DeleteWithContext(ctx)
	if err != nil {
		return
	}
	return nil
}
func (s *Service) get(ctx context.Context, name string, opt *pairServiceGet) (store typ.Storager, err error) {
	pairs := append(opt.pairs, ps.WithName(name))

	store, err = s.newStorage(pairs...)
	if err != nil {
		return
	}
	return
}

func (s *Service) list(ctx context.Context, opt *pairServiceList) (it *typ.StoragerIterator, err error) {
	input := &service.ListBucketsInput{
		Offset: service.Int(0),
	}
	if opt.HasLocation {
		input.Location = &opt.Location
	}

	var output *service.ListBucketsOutput

	fn := typ.NextStoragerFunc(func(page *typ.StoragerPage) error {
		output, err = s.service.ListBucketsWithContext(ctx, input)
		if err != nil {
			return err
		}

		for _, v := range output.Buckets {
			store, err := s.newStorage(ps.WithName(*v.Name), ps.WithLocation(*v.Location))
			if err != nil {
				return err
			}
			page.Data = append(page.Data, store)
		}

		*input.Offset += len(output.Buckets)
		if *input.Offset >= service.IntValue(output.Count) {
			return typ.IterateDone
		}

		return nil
	})

	return typ.NewStoragerIterator(fn), nil
}
