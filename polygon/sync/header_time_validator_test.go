package sync

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

var amoySpan1280Json = `
{
  "span_id": 1280,
  "start_block": 8185856,
  "end_block": 8192255,
  "validator_set": {
    "validators": [
      {
        "ID": 16,
        "startEpoch": 5457,
        "endEpoch": 0,
        "nonce": 28,
        "power": 1300197,
        "pubKey": "0x04d0c6e650238a566873726ca0e7be81cdf79d587a1278d0a47e9f27f4997d3ef47bacb557c56d1222dc63d31451f8cf467849a2b421e641b9a397573f7486a656",
        "signer": "0x02f615e95563ef16f10354dba9e584e58d2d4314",
        "last_updated": "608485000381",
        "jailed": false,
        "accum": 11416548
      },
      {
        "ID": 15,
        "startEpoch": 4288,
        "endEpoch": 0,
        "nonce": 10,
        "power": 1300001,
        "pubKey": "0x040b391186d8991472a32127435f139fa84ddfae556a693720b79432cb7a65f7c6f05f0bed6c4814f41f3b688b5d00fa6143572647e84237c1594f4fca8b3026d2",
        "signer": "0x033aaded92ebfec6c818624f28a8e4b3c571ab6f",
        "last_updated": "600761600019",
        "jailed": false,
        "accum": 10093635
      },
      {
        "ID": 13,
        "startEpoch": 3676,
        "endEpoch": 0,
        "nonce": 20,
        "power": 1310092,
        "pubKey": "0x047bc430cbd0e704219ee6204c544ca94f5ba89efd32a3e6487b1039195b7da55f897b620bd9c1a7f8cbc5f90b85ea3346e259d5b5d480c10912a0c3fbb20c37a4",
        "signer": "0x04ba3ef4c023c1006019a0f9baf6e70455e41fcf",
        "last_updated": "601345600015",
        "jailed": false,
        "accum": 14926693
      },
      {
        "ID": 6,
        "startEpoch": 246,
        "endEpoch": 0,
        "nonce": 277,
        "power": 1463001,
        "pubKey": "0x04ac47e6c02a409a4b7bec875f1dbe0848ff209eccd41dd4606f33cda6ebd44e319a82c8609efcee9bdbe6157365100e2ad1a8ce6dd52d9bed4f935295bf352fdb",
        "signer": "0x09207a6efee346cb3e4a54ac18523e3715d38b3f",
        "last_updated": "587036300319",
        "jailed": false,
        "accum": 1381777
      },
      {
        "ID": 14,
        "startEpoch": 3863,
        "endEpoch": 0,
        "nonce": 30,
        "power": 1300400,
        "pubKey": "0x04e8acf9583307d4d840c695fe53d3edcc9a7e1fd6ddfc514baa415ec83e569e69344e0336e62b05fef2230d4e4cc83cf583ada8e83271efb6c790befac0add019",
        "signer": "0x22b64229c41429a023549fdab3385893b579327a",
        "last_updated": "598326000006",
        "jailed": false,
        "accum": 1411714
      },
      {
        "ID": 10,
        "startEpoch": 2966,
        "endEpoch": 0,
        "nonce": 55,
        "power": 1325107,
        "pubKey": "0x049b69a79f9cfd815b8ffb8478577b4338d8041cff7e834263b527b9d094921b302f8e8b255c7b4c3527671a343b8711c79d88c23dd38f4873fd9a9499a5395e96",
        "signer": "0x4631753190f2f5a15a7ba172bbac102b7d95fa22",
        "last_updated": "604373100165",
        "jailed": false,
        "accum": -2618784
      },
      {
        "ID": 4,
        "startEpoch": 187,
        "endEpoch": 0,
        "nonce": 931,
        "power": 3946711,
        "pubKey": "0x04381a045d0abe77c9d561086bd1c8ad68eff21b08d865a4928563ce43d7b9d3679c9d90bd110087ca0234eba2e869c9869404f7d35f1d24062a33a1727dca78c2",
        "signer": "0x4ad84f7014b7b44f723f284a85b1662337971439",
        "last_updated": "608930100006",
        "jailed": false,
        "accum": -9290359
      },
      {
        "ID": 9,
        "startEpoch": 2910,
        "endEpoch": 0,
        "nonce": 339,
        "power": 1321933,
        "pubKey": "0x04c71aa31d66fce65801efc8b76502e60f01881b8d391da7e8d5573878d5500852e0149b2648802fefe3e306d9aaf2d6b462c6b07f64acc555d2baafd529e73aaf",
        "signer": "0x4ca9ff871c7aa1e7b64e1eae110835f68d6a0bd4",
        "last_updated": "609077600013",
        "jailed": false,
        "accum": -12229413
      },
      {
        "ID": 1,
        "startEpoch": 0,
        "endEpoch": 0,
        "nonce": 504,
        "power": 3956215,
        "pubKey": "0x04dc19fdf9a82fd5c4327f31b96b6bbe0b9d44564ad89c2139db47c5cb2def87ac584fc05117663de2f17ae5ee50eced7283a596e10aaf33fb34c4cf5f98e4fda7",
        "signer": "0x6ab3d36c46ecfb9b9c0bd51cb1c3da5a2c81cea6",
        "last_updated": "608859500009",
        "jailed": false,
        "accum": -1402956
      },
      {
        "ID": 12,
        "startEpoch": 3381,
        "endEpoch": 0,
        "nonce": 28,
        "power": 1300223,
        "pubKey": "0x0425172023c08112aa9633273951fcb5276e2db7096ce4b842465f34f5b13ebe66f800f75f888d5ff1762ea49d1546ceaa62c23c0cefbe68b312e36b6ce55c8499",
        "signer": "0x6c095a53250dd250797ff915a716cca690ad8842",
        "last_updated": "609077600007",
        "jailed": false,
        "accum": 8813833
      },
      {
        "ID": 5,
        "startEpoch": 187,
        "endEpoch": 0,
        "nonce": 8,
        "power": 1475350,
        "pubKey": "0x04749b4e69c5fdf4a9a3a50d81fb24a7ff48a3da36f155dea9be0991a6d00c2557aa5acd44b16880e586f4f5605102a8cc61fd6acd1c9416688cc1b1cbf522961b",
        "signer": "0x6dc2dd54f24979ec26212794c71afefed722280c",
        "last_updated": "587036300305",
        "jailed": false,
        "accum": -2817182
      },
      {
        "ID": 7,
        "startEpoch": 246,
        "endEpoch": 0,
        "nonce": 896,
        "power": 3753702,
        "pubKey": "0x044fd620bf91dcffbc6251817f1a8f75bfa35a559d022a1c3ce058357a17a983a5434f90889b9d0c830b843aa8becb0ebd2f51105694627ea95c4d18183de76f99",
        "signer": "0x915a2284d28bd93de7d6f31173b981204bb666e6",
        "last_updated": "609046200005",
        "jailed": false,
        "accum": -3627294
      },
      {
        "ID": 11,
        "startEpoch": 3324,
        "endEpoch": 0,
        "nonce": 410,
        "power": 1471139,
        "pubKey": "0x045ffe6c668a6e54c9b1825e8e5e9c0aaf714240ead3a06ca97cf914321411271a283d90ae88886dd3fe69f43f2691e432f52ac1373a88aa4ee15b5826107a501f",
        "signer": "0xac75d6efec891724b88b916b36e2ef38bcbec73f",
        "last_updated": "587036300312",
        "jailed": false,
        "accum": -8267418
      },
      {
        "ID": 8,
        "startEpoch": 2669,
        "endEpoch": 0,
        "nonce": 32,
        "power": 1310815,
        "pubKey": "0x0448f2322aa4f44a87214860d9fb29b4a4b385a2d5ad282a304cbe9d7a32bb777c65663225859e04efe97b4d1217b1c9518dc6c7b2165c3a6cd0ecddb3c7816d17",
        "signer": "0xbb583a9dde59ca64aaa14807f37a4c665c0d72c7",
        "last_updated": "587665300004",
        "jailed": false,
        "accum": -87190
      },
      {
        "ID": 17,
        "startEpoch": 5487,
        "endEpoch": 0,
        "nonce": 9,
        "power": 1350040,
        "pubKey": "0x043fdb65db2cafa7ba9ca2b89d43d8be4d51dd67185d5af56f3e21bfbbec0be000797a7217bab3d99d3ed81b24818eb4a6b4ee15bc0145d859d2c18360662de0bf",
        "signer": "0xede32b0c9587b92ede83665477f7ec261fd85f0a",
        "last_updated": "587036300333",
        "jailed": false,
        "accum": -7703596
      }
    ],
    "proposer": {
      "ID": 9,
      "startEpoch": 2910,
      "endEpoch": 0,
      "nonce": 339,
      "power": 1321933,
      "pubKey": "0x04c71aa31d66fce65801efc8b76502e60f01881b8d391da7e8d5573878d5500852e0149b2648802fefe3e306d9aaf2d6b462c6b07f64acc555d2baafd529e73aaf",
      "signer": "0x4ca9ff871c7aa1e7b64e1eae110835f68d6a0bd4",
      "last_updated": "609077600013",
      "jailed": false,
      "accum": -12229413
    }
  },
  "selected_producers": [
    {
      "ID": 14,
      "startEpoch": 3863,
      "endEpoch": 0,
      "nonce": 30,
      "power": 1,
      "pubKey": "0x04e8acf9583307d4d840c695fe53d3edcc9a7e1fd6ddfc514baa415ec83e569e69344e0336e62b05fef2230d4e4cc83cf583ada8e83271efb6c790befac0add019",
      "signer": "0x22b64229c41429a023549fdab3385893b579327a",
      "last_updated": "598326000006",
      "jailed": false,
      "accum": 0
    },
    {
      "ID": 10,
      "startEpoch": 2966,
      "endEpoch": 0,
      "nonce": 61,
      "power": 1,
      "pubKey": "0x049b69a79f9cfd815b8ffb8478577b4338d8041cff7e834263b527b9d094921b302f8e8b255c7b4c3527671a343b8711c79d88c23dd38f4873fd9a9499a5395e96",
      "signer": "0x4631753190f2f5a15a7ba172bbac102b7d95fa22",
      "last_updated": "605947600003",
      "jailed": false,
      "accum": 0
    },
    {
      "ID": 1,
      "startEpoch": 0,
      "endEpoch": 0,
      "nonce": 505,
      "power": 2,
      "pubKey": "0x04dc19fdf9a82fd5c4327f31b96b6bbe0b9d44564ad89c2139db47c5cb2def87ac584fc05117663de2f17ae5ee50eced7283a596e10aaf33fb34c4cf5f98e4fda7",
      "signer": "0x6ab3d36c46ecfb9b9c0bd51cb1c3da5a2c81cea6",
      "last_updated": "608990800100",
      "jailed": false,
      "accum": 0
    },
    {
      "ID": 12,
      "startEpoch": 3381,
      "endEpoch": 0,
      "nonce": 28,
      "power": 1,
      "pubKey": "0x0425172023c08112aa9633273951fcb5276e2db7096ce4b842465f34f5b13ebe66f800f75f888d5ff1762ea49d1546ceaa62c23c0cefbe68b312e36b6ce55c8499",
      "signer": "0x6c095a53250dd250797ff915a716cca690ad8842",
      "last_updated": "609077600007",
      "jailed": false,
      "accum": 0
    },
    {
      "ID": 5,
      "startEpoch": 187,
      "endEpoch": 0,
      "nonce": 8,
      "power": 1,
      "pubKey": "0x04749b4e69c5fdf4a9a3a50d81fb24a7ff48a3da36f155dea9be0991a6d00c2557aa5acd44b16880e586f4f5605102a8cc61fd6acd1c9416688cc1b1cbf522961b",
      "signer": "0x6dc2dd54f24979ec26212794c71afefed722280c",
      "last_updated": "587036300305",
      "jailed": false,
      "accum": 0
    },
    {
      "ID": 7,
      "startEpoch": 246,
      "endEpoch": 0,
      "nonce": 896,
      "power": 3,
      "pubKey": "0x044fd620bf91dcffbc6251817f1a8f75bfa35a559d022a1c3ce058357a17a983a5434f90889b9d0c830b843aa8becb0ebd2f51105694627ea95c4d18183de76f99",
      "signer": "0x915a2284d28bd93de7d6f31173b981204bb666e6",
      "last_updated": "609046200005",
      "jailed": false,
      "accum": 0
    },
    {
      "ID": 11,
      "startEpoch": 3324,
      "endEpoch": 0,
      "nonce": 410,
      "power": 1,
      "pubKey": "0x045ffe6c668a6e54c9b1825e8e5e9c0aaf714240ead3a06ca97cf914321411271a283d90ae88886dd3fe69f43f2691e432f52ac1373a88aa4ee15b5826107a501f",
      "signer": "0xac75d6efec891724b88b916b36e2ef38bcbec73f",
      "last_updated": "587036300312",
      "jailed": false,
      "accum": 0
    },
    {
      "ID": 17,
      "startEpoch": 5487,
      "endEpoch": 0,
      "nonce": 46,
      "power": 1,
      "pubKey": "0x043fdb65db2cafa7ba9ca2b89d43d8be4d51dd67185d5af56f3e21bfbbec0be000797a7217bab3d99d3ed81b24818eb4a6b4ee15bc0145d859d2c18360662de0bf",
      "signer": "0xede32b0c9587b92ede83665477f7ec261fd85f0a",
      "last_updated": "608445000007",
      "jailed": false,
      "accum": 0
    }
  ],
  "bor_chain_id": "80002"
}
`

func TestHeaderTimeValidator(t *testing.T) {
	t.Run("1st sprint within span 1280", func(t *testing.T) {
		// test using amoy block 8185856 validated by 0x915a2284D28BD93de7d6F31173b981204bb666E6
		header := &types.Header{
			Number: big.NewInt(8185856),
			Time:   1718194023,
		}
		parent := &types.Header{
			Number: big.NewInt(8185855),
			Time:   1718194019,
		}
		amoySpan1280 := heimdall.Span{}
		err := json.Unmarshal([]byte(amoySpan1280Json), &amoySpan1280)
		require.NoError(t, err)
		borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
		spansCache := &SpansCache{
			spans: map[uint64]*heimdall.Span{
				header.Number.Uint64(): &amoySpan1280,
			},
		}
		signaturesCache, err := lru.NewARC[common.Hash, common.Address](1)
		require.NoError(t, err)
		signaturesCache.Add(header.Hash(), common.HexToAddress("0x915a2284D28BD93de7d6F31173b981204bb666E6"))

		validator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache)
		err = validator.ValidateHeaderTime(header, time.Now(), parent)
		require.NoError(t, err)
	})

	t.Run("2nd sprint within span 1280", func(t *testing.T) {
		// test using amoy block 8185872 validated by 0x6aB3d36C46ecFb9B9c0bD51CB1c3da5A2C81cea6
		header := &types.Header{
			Number: big.NewInt(8185872),
			Time:   1718194057,
		}
		parent := &types.Header{
			Number: big.NewInt(8185871),
			Time:   1718194053,
		}
		amoySpan1280 := heimdall.Span{}
		err := json.Unmarshal([]byte(amoySpan1280Json), &amoySpan1280)
		require.NoError(t, err)
		borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
		spansCache := &SpansCache{
			spans: map[uint64]*heimdall.Span{
				header.Number.Uint64(): &amoySpan1280,
			},
		}
		signaturesCache, err := lru.NewARC[common.Hash, common.Address](1)
		require.NoError(t, err)
		signaturesCache.Add(header.Hash(), common.HexToAddress("0x6aB3d36C46ecFb9B9c0bD51CB1c3da5A2C81cea6"))

		validator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache)
		err = validator.ValidateHeaderTime(header, time.Now(), parent)
		require.NoError(t, err)
	})

	t.Run("3rd sprint within span 1280", func(t *testing.T) {
		// test using amoy block 8185888 validated by 0x6aB3d36C46ecFb9B9c0bD51CB1c3da5A2C81cea6
		header := &types.Header{
			Number: big.NewInt(8185888),
			Time:   1718194091,
		}
		parent := &types.Header{
			Number: big.NewInt(8185887),
			Time:   1718194087,
		}
		amoySpan1280 := heimdall.Span{}
		err := json.Unmarshal([]byte(amoySpan1280Json), &amoySpan1280)
		require.NoError(t, err)
		borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
		spansCache := &SpansCache{
			spans: map[uint64]*heimdall.Span{
				header.Number.Uint64(): &amoySpan1280,
			},
		}
		signaturesCache, err := lru.NewARC[common.Hash, common.Address](1)
		require.NoError(t, err)
		signaturesCache.Add(header.Hash(), common.HexToAddress("0x4631753190F2F5A15A7ba172bbaC102B7d95FA22"))

		validator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache)
		err = validator.ValidateHeaderTime(header, time.Now(), parent)
		require.NoError(t, err)
	})

	t.Run("91st sprint within span 1280", func(t *testing.T) {
		// test using amoy block 8187309 validated by 0x915a2284D28BD93de7d6F31173b981204bb666E6
		header := &types.Header{
			Number: big.NewInt(8187309),
			Time:   1718197109,
		}
		parent := &types.Header{
			Number: big.NewInt(8187308),
			Time:   1718197107,
		}
		amoySpan1280 := heimdall.Span{}
		err := json.Unmarshal([]byte(amoySpan1280Json), &amoySpan1280)
		require.NoError(t, err)
		borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
		spansCache := &SpansCache{
			spans: map[uint64]*heimdall.Span{
				header.Number.Uint64(): &amoySpan1280,
			},
		}
		signaturesCache, err := lru.NewARC[common.Hash, common.Address](1)
		require.NoError(t, err)
		signaturesCache.Add(header.Hash(), common.HexToAddress("0x915a2284D28BD93de7d6F31173b981204bb666E6"))

		validator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache)
		err = validator.ValidateHeaderTime(header, time.Now(), parent)
		require.NoError(t, err)
	})
}

func generateDifficultyReport(vs *valset.ValidatorSet, blockNum uint64) DifficultyReport {
	difficulties := make([]DifficultyKV, 0, len(vs.Validators))
	vs.Iterate(func(index int, val *valset.Validator) bool {
		difficulty, err := vs.Difficulty(val.Address)
		if err != nil {
			panic(err)
		}
		difficulties = append(difficulties, DifficultyKV{
			Validator:  val.Address,
			Difficulty: difficulty,
		})
		return false
	})
	sort.Slice(difficulties, func(i, j int) bool {
		return difficulties[i].Difficulty > difficulties[j].Difficulty
	})
	return DifficultyReport{
		Difficulties: difficulties,
		BlockNum:     blockNum,
	}
}

type DifficultyReport struct {
	Difficulties []DifficultyKV
	BlockNum     uint64
}

func (dr DifficultyReport) PrintCsv(sb *strings.Builder) {
	for _, difficulty := range dr.Difficulties {
		sb.WriteString(strconv.FormatUint(dr.BlockNum, 10))
		sb.WriteRune(',')
		sb.WriteString(difficulty.Validator.String())
		sb.WriteRune(',')
		sb.WriteString(strconv.FormatUint(difficulty.Difficulty, 10))
		sb.WriteRune(',')
		sb.WriteRune('\n')
	}
}

type DifficultyKV struct {
	Validator  common.Address
	Difficulty uint64
}

func TestProduceDifficulties(t *testing.T) {
	borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
	amoySpan1280 := heimdall.Span{}
	err := json.Unmarshal([]byte(amoySpan1280Json), &amoySpan1280)
	require.NoError(t, err)

	var sb strings.Builder

	for headerNum := amoySpan1280.StartBlock; headerNum <= amoySpan1280.EndBlock; headerNum++ {
		validatorSet := valset.NewValidatorSet(amoySpan1280.Producers())
		currentSprintNumber := borConfig.CalculateSprintNumber(headerNum)
		fmt.Printf("currentSprintNumber=%d\n", currentSprintNumber)
		firstSprintNumber := borConfig.CalculateSprintNumber(amoySpan1280.StartBlock)
		fmt.Printf("firstSprintNumber=%d\n", firstSprintNumber)
		incrementsNeeded := int(currentSprintNumber - firstSprintNumber)
		fmt.Printf("incrementsNeeded=%d\n", incrementsNeeded)
		if incrementsNeeded > 0 {
			validatorSet.IncrementProposerPriority(incrementsNeeded)
		}

		report := generateDifficultyReport(validatorSet, headerNum)
		report.PrintCsv(&sb)
	}

	file, err := os.Create("/Users/taratorio/difficulty-report.csv")
	require.NoError(t, err)
	defer func() {
		err = file.Close()
		require.NoError(t, err)
	}()

	_, err = file.WriteString(sb.String())
	require.NoError(t, err)
}

func TestProduceDifficultiesV2(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	heimdallClient := heimdall.NewHeimdallClient("https://heimdall-api-amoy.polygon.technology", log.New())
	borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
	amoySpan1280 := heimdall.Span{}
	err := json.Unmarshal([]byte(amoySpan1280Json), &amoySpan1280)
	require.NoError(t, err)

	var sb strings.Builder

	var validatorSet *valset.ValidatorSet
	for id := uint64(0); id <= 1280; id++ {
		span, err := heimdallClient.FetchSpan(ctx, id)
		require.NoError(t, err)
		if validatorSet == nil {
			validatorSet = valset.NewValidatorSet(span.Producers())
		} else {
			validatorSet = bor.GetUpdatedValidatorSet(validatorSet, span.Producers(), log.New())
		}
		if id == 1280 {
			break
		}
		headerNum := min(span.EndBlock, amoySpan1280.StartBlock-1)
		currentSprintNumber := borConfig.CalculateSprintNumber(headerNum)
		firstSprintNumber := borConfig.CalculateSprintNumber(span.StartBlock)
		incrementsNeeded := int(currentSprintNumber-firstSprintNumber) + 1
		validatorSet.IncrementProposerPriority(incrementsNeeded)
	}
	if validatorSet == nil {
		require.Fail(t, "validator set is nil")
		return
	}

	for headerNum := amoySpan1280.StartBlock; headerNum <= amoySpan1280.EndBlock; headerNum++ {
		validatorSetCopy := validatorSet.Copy()
		currentSprintNumber := borConfig.CalculateSprintNumber(headerNum)
		firstSprintNumber := borConfig.CalculateSprintNumber(amoySpan1280.StartBlock)
		incrementsNeeded := int(currentSprintNumber-firstSprintNumber) + 1
		validatorSetCopy.IncrementProposerPriority(incrementsNeeded)

		report := generateDifficultyReport(validatorSetCopy, headerNum)
		report.PrintCsv(&sb)
	}

	file, err := os.Create("/Users/taratorio/difficulty-report-v2.csv")
	require.NoError(t, err)
	defer func() {
		err = file.Close()
		require.NoError(t, err)
	}()

	_, err = file.WriteString(sb.String())
	require.NoError(t, err)
}

var curlCmdTemplate = `
curl https://polygon-amoy-bor-rpc.publicnode.com \
-X POST \
-H "Content-Type: application/json" \
-o %s \
--data '{"jsonrpc":"2.0","method":"bor_getSnapshotProposerSequence","params":["%s"], "id":1}'
`

func curlArgs(blockNum uint64) ([]string, string) {
	fileName := fmt.Sprintf("/Users/taratorio/bor_getSnapshotProposerSequence_%d.txt", blockNum)
	return []string{
		"https://polygon-amoy-bor-rpc.publicnode.com",
		"-X",
		"POST",
		"-H",
		`"Content-Type: application/json"`,
		"-o",
		fileName,
		"--data",
		fmt.Sprintf(`'{"jsonrpc":"2.0","method":"bor_getSnapshotProposerSequence","params":["0x%s"], "id":1}'`, strconv.FormatUint(blockNum, 16)),
	}, fileName
}

// Generated by curl-to-Go: https://mholt.github.io/curl-to-go

// curl https://polygon-amoy-bor-rpc.publicnode.com -X POST -H "Content-Type: application/json" -o /Users/taratorio/bor_getSnapshotProposerSequence_8185856.txt --data '{"jsonrpc":"2.0","method":"bor_getSnapshotProposerSequence","params":["0x7ce800"], "id":1}'

type Payload struct {
	Jsonrpc string   `json:"jsonrpc"`
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      int      `json:"id"`
}

type Response struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  Result `json:"result"`
}

type Result struct {
	Signers []SignersKV
	Diff    int
	Author  common.Address
}

type SignersKV struct {
	Signer     common.Address
	Difficulty uint64
}

func fetchGoldenDifficultyReport(blockNum uint64) DifficultyReport {
	data := Payload{
		Jsonrpc: "2.0",
		Method:  "bor_getSnapshotProposerSequence",
		Params:  []string{fmt.Sprintf("0x%s", strconv.FormatUint(blockNum, 16))},
		ID:      1,
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", "https://polygon-amoy-bor-rpc.publicnode.com", body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var unmarshalledResp Response
	err = json.Unmarshal(respBody, &unmarshalledResp)
	if err != nil {
		panic(err)
	}

	difficulties := make([]DifficultyKV, len(unmarshalledResp.Result.Signers))
	for i, signer := range unmarshalledResp.Result.Signers {
		difficulties[i] = DifficultyKV{
			Validator:  signer.Signer,
			Difficulty: signer.Difficulty,
		}
	}
	return DifficultyReport{
		Difficulties: difficulties,
		BlockNum:     blockNum,
	}
}

func TestFetchGoldenDifficulties(t *testing.T) {
	amoySpan1280 := heimdall.Span{}
	err := json.Unmarshal([]byte(amoySpan1280Json), &amoySpan1280)
	require.NoError(t, err)

	var sb strings.Builder

	for headerNum := amoySpan1280.StartBlock; headerNum <= amoySpan1280.EndBlock; headerNum++ {
		//curlArgsText, fileName := curlArgs(headerNum)
		//cmd := exec.Command("curl", curlArgsText...)
		//println(cmd.String())
		//err := cmd.Run()
		//require.NoError(t, err)
		//
		//jsonBytes, err := os.ReadFile(fileName)
		//require.NoError(t, err)
		//println(string(jsonBytes))

		report := fetchGoldenDifficultyReport(headerNum)
		report.PrintCsv(&sb)
		time.Sleep(200 * time.Millisecond)
	}

	file, err := os.Create("/Users/taratorio/difficulty-report-golden.csv")
	require.NoError(t, err)
	defer func() {
		err = file.Close()
		require.NoError(t, err)
	}()

	_, err = file.WriteString(sb.String())
	require.NoError(t, err)
}

func TestGetProducersFromHeaderExtraBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := log.New()
	nodeCfg := nodecfg.Config{
		Dirs: datadir.Dirs{
			DataDir: "/Volumes/ThunderBlade/erigon3-polygon-sync-amoy",
		},
	}

	db, err := node.OpenDatabase(ctx, &nodeCfg, kv.ChainDB, "" /* name */, true /* readonly */, logger)
	require.NoError(t, err)

	//headerHash := common.HexToHash("0xc5f25d60a50c1686909091d86f1fd955e905295d62ae1167017d10629b26bc06")
	//headerNumber := uint64(8185855)

	//headerHash := common.HexToHash("0xbd3a1ab137da0a6ebb25dc813bedefb15c90da979d907414e8d10ba73c555980")
	//headerNumber := uint64(8185871)

	//headerHash := common.HexToHash("0x4193fa9c9b33cd085bdc27582261fccb6f41fac35e295a579e0a1d2a4a9a8883")
	headerNumber := uint64(8185887)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	//header := rawdb.ReadHeader(roTx, headerHash, headerNumber)
	var header *types.Header
	if header == nil {
		//panic("header not found")
		//println("header not found - using manual fallback")

		// 8185855
		//extra, err := hex.DecodeString("d78301030083626f7288676f312e32302e37856c696e75780000000000000000f90147b9014022b64229c41429a023549fdab3385893b579327a00000000000000000000000000000000000000014631753190f2f5a15a7ba172bbac102b7d95fa2200000000000000000000000000000000000000016ab3d36c46ecfb9b9c0bd51cb1c3da5a2c81cea600000000000000000000000000000000000000026c095a53250dd250797ff915a716cca690ad884200000000000000000000000000000000000000016dc2dd54f24979ec26212794c71afefed722280c0000000000000000000000000000000000000001915a2284d28bd93de7d6f31173b981204bb666e60000000000000000000000000000000000000003ac75d6efec891724b88b916b36e2ef38bcbec73f0000000000000000000000000000000000000001ede32b0c9587b92ede83665477f7ec261fd85f0a0000000000000000000000000000000000000001c3c0c0c0cb2b6b7a7dc2e803286ffcf6d29076f2c43aded0328a4cda5ba33e58daa1c36d4d1c0a3fd700509a06010a06a43e1496a171dfd1b0ca5da3ad36121f8b562a7f01")
		// 8185871
		//extra, err := hex.DecodeString("d78301030383626f7288676f312e32322e33856c696e75780000000000000000f90146b9014022b64229c41429a023549fdab3385893b579327a00000000000000000000000000000000000000014631753190f2f5a15a7ba172bbac102b7d95fa2200000000000000000000000000000000000000016ab3d36c46ecfb9b9c0bd51cb1c3da5a2c81cea600000000000000000000000000000000000000026c095a53250dd250797ff915a716cca690ad884200000000000000000000000000000000000000016dc2dd54f24979ec26212794c71afefed722280c0000000000000000000000000000000000000001915a2284d28bd93de7d6f31173b981204bb666e60000000000000000000000000000000000000003ac75d6efec891724b88b916b36e2ef38bcbec73f0000000000000000000000000000000000000001ede32b0c9587b92ede83665477f7ec261fd85f0a0000000000000000000000000000000000000001c2c0c0e456c6225895c9a6b34243989364ec5b9243a9affba07a8649540efbfb3f941a77f3e92fc3306660731ee9684b190dd765d46030fa6be87251a205591367cdc400")
		// 8185887
		extra, err := hex.DecodeString("d78301030083626f7288676f312e32302e37856c696e75780000000000000000f90146b9014022b64229c41429a023549fdab3385893b579327a00000000000000000000000000000000000000014631753190f2f5a15a7ba172bbac102b7d95fa2200000000000000000000000000000000000000016ab3d36c46ecfb9b9c0bd51cb1c3da5a2c81cea600000000000000000000000000000000000000026c095a53250dd250797ff915a716cca690ad884200000000000000000000000000000000000000016dc2dd54f24979ec26212794c71afefed722280c0000000000000000000000000000000000000001915a2284d28bd93de7d6f31173b981204bb666e60000000000000000000000000000000000000003ac75d6efec891724b88b916b36e2ef38bcbec73f0000000000000000000000000000000000000001ede32b0c9587b92ede83665477f7ec261fd85f0a0000000000000000000000000000000000000001c2c0c0e4cec52b1ed63ae6301122ac28edcc8abd86b419df04bb492b2a9f818072e5a95587541cee575fb092e70b7b3b45a37ea80413b66aa3e77b901134a499c5542801")
		if err != nil {
			panic(err)
		}

		header = &types.Header{
			Number: new(big.Int).SetUint64(headerNumber),
			Extra:  extra,
		}
	}

	validatorBytes := bor.GetValidatorBytes(header, params.AmoyChainConfig.Bor.(*borcfg.BorConfig))
	newVals, _ := valset.ParseValidators(validatorBytes)
	selectedProducers := make([]valset.Validator, len(newVals))
	for i, val := range newVals {
		selectedProducers[i] = *val
	}

	span := heimdall.Span{
		SelectedProducers: selectedProducers,
	}

	jsonBytes, err := json.Marshal(span)
	if err != nil {
		panic(err)
	}

	println(fmt.Printf("%v", newVals))
	println(string(jsonBytes))
}
