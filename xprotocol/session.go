package xprotocol

import (
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
)

type XSession struct {
	ctx driver.QueryCtx
}

func CreateXSession(id uint32, ctx driver.QueryCtx, pkt *xpacketio.XPacketIO, skipAuth bool) *XSession {
	return &XSession{
		ctx: ctx,
	}
}

func (xs *XSession) HandleMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	switch msgType {
	case Mysqlx.ClientMessages_SQL_STMT_EXECUTE:
		if err := xs.DealSQLStmtExecute(msgType, payload); err != nil {
			return err
		}
	default:
		return errors.Errorf("unknown message type %d", msgType)
	}

	return nil
}

func (xs *XSession) DealSQLStmtExecute (msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var msg Mysqlx_Sql.StmtExecute
	if err := msg.Unmarshal(payload); err != nil {
		return err
	}

	switch msg.GetNamespace() {
	case "xplugin":
	case "mysqlx":
	case "sql", "":
		sql := string(msg.GetStmt())
		xs.ctx.Execute(sql)
	default:
		return errors.New("unknown namespace")
	}
	return nil
}
