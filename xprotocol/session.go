package xprotocol

import (
	"github.com/pingcap/tidb/xprotocol/auth"
	"github.com/pingcap/tidb/xprotocol/sql"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/arena"
)

type XSession struct {
	auth *auth.XAuth
	xsql *sql.XSql
}

func CreateXSession(alloc *arena.Allocator, id uint32, ctx driver.QueryCtx, pkt *xpacketio.XPacketIO) *XSession {
	return &XSession{
		auth: auth.CreateAuth(id, ctx, pkt),
		xsql: sql.CreateContext(alloc, ctx, pkt),
	}
}

func (xs *XSession) HandleMessage(tp int32, payload []byte) error {
	msgType := Mysqlx.ClientMessages_Type(tp)
	switch msgType {
	case Mysqlx.ClientMessages_SESS_CLOSE, Mysqlx.ClientMessages_CON_CLOSE, Mysqlx.ClientMessages_SESS_RESET:
		if err := xs.auth.HandleReadyMessage(msgType, payload); err != nil {
			return err
		}
	case Mysqlx.ClientMessages_SQL_STMT_EXECUTE:
		if err := xs.xsql.DealSQLStmtExecute(msgType, payload); err != nil {
			return err
		}
	default:
		return errors.Errorf("unknown message type %d", tp)
	}

	return nil
}

