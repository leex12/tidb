package xprotocol

import (
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Connection"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tidb/xprotocol/capability"
	"fmt"
)

func getCapability(handler capability.Handler) *Mysqlx_Connection.Capability {
	return handler.Get()
}

func GetCapabilities() *Mysqlx_Connection.Capabilities {
	authHandler := &capability.HandleAuthMech {
		Values: []string{"MYSQL41"},
	}
	docHandler := &capability.HandlerReadOnlyValue{
		Name: "doc.formats",
		Value: "text",
	}
	nodeHandler := &capability.HandlerReadOnlyValue{
		Name: "node_type",
		Value: "mysql",
	}
	pwdHandler := &capability.HandlerExpiredPasswords{
		Name: "client.pwd_expire_ok",
		Expired: true,
	}
	caps := Mysqlx_Connection.Capabilities{
		Capabilities: []*Mysqlx_Connection.Capability{
			getCapability(authHandler),
			getCapability(docHandler),
			getCapability(nodeHandler),
			getCapability(pwdHandler),
		},
	}
	return &caps
}

func DealInitCapabilitiesSet (tp Mysqlx.ClientMessages_Type, msg []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_SET {
		return errors.New("bad capabilities set")
	}
	var set Mysqlx_Connection.CapabilitiesSet
	if err := set.Unmarshal(msg); err != nil {
		return errors.Trace(err)
	}
	if set.GetCapabilities() == nil {
		return errors.New("bad capabilities set")
	}
	caps := set.GetCapabilities().GetCapabilities()
	if caps == nil {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetName() != "client.pwd_expire_ok" {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_BOOL {
		return errors.New("bad capabilities set")
	}
	if !caps[0].GetValue().GetScalar().GetVBool() {
		return errors.New("bad capabilities set")
	}
	return nil
}

func DealCapabilitiesGet (tp Mysqlx.ClientMessages_Type, _ []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_GET {
		return errors.New("bad capabilities get")
	}
	return nil
}

func DealSecCapabilitiesSet (tp Mysqlx.ClientMessages_Type, msg []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_SET {
		return errors.New("bad capabilities set")
	}
	var set Mysqlx_Connection.CapabilitiesSet
	if err := set.Unmarshal(msg); err != nil {
		return errors.Trace(err)
	}
	if set.GetCapabilities() == nil {
		return errors.New("bad capabilities set")
	}
	caps := set.GetCapabilities().GetCapabilities()
	if caps == nil {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetName() != "tls" {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_BOOL {
		return errors.New("bad capabilities set")
	}
	if !caps[0].GetValue().GetScalar().GetVBool() {
		return errors.New("bad capabilities set")
	}
	return nil
}

func ErrorReport() *Mysqlx.Error {
	code := new(uint32)
	*code = 5001
	sqlState := new(string)
	*sqlState = "HY000"
	msg := new(string)
	*msg = fmt.Sprintf("Capability prepare failed for 'tls'")
	errMsg := Mysqlx.Error{
		Severity: Mysqlx.Error_ERROR.Enum(),
		Code:     code,
		SqlState: sqlState,
		Msg:      msg,
	}
	return &errMsg
}
