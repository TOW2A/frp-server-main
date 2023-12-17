package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gopkg.in/ini.v1"
)

//	type Userdata_db struct {
//		UserId    int    `db:"userid"`
//		Username  string `db:"username"`
//		Password  string `db:"password"`
//		Telephone string `db:"telephone"`
//		Email     string `db:"email"`
//		Nodesnum string `db:"Nodesnum"`
//		Nodesnum  int    `db:"Nodesnum"`Nodesnum
//	}

// type Userdata_jsonin struct {
// 	Type      string `json:"type"`
// 	Optype    string `json:"Optype"`
// 	UserId    string `json:"userid"`
// 	Username  string `json:"username"`
// 	Telephone string `json:"telephone"`
// 	Email     string `json:"email"`
// 	Password  string `json:"password"`
// 	Nodesnum string `json:"Nodesnum"`
// }

type Nodes_Connect struct {
	Type      string `json:"type"`
	UserId    string `json:"userId"`
	Nodesdata string `json:"Nodesdata"`
	Nodetoken string `json:"nodetoken"`
	NodeIP    string `json:"nodeip"`
	Nodeport  string `json:"nodeport"`
	Speed     string `json:"speed"`
	Nodeid    string `json:"nodeid"`
}

type Userdata_json struct {
	Type        string `json:"type"`
	Optype      string `json:"optype"`
	UserId      string `json:"userId"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Email       string `json:"email"`
	Telephone   string `json:"telephone"`
	Nodesnum    string `json:"nodesnum"`
	Nodesdata   string `json:"nodesdata"`
	Nodesid     string `json:"nodesid"`
	Member      string `json:"member"`
	NodesMaxNum string `json:"nodesmaxnum"`
	Errortypes  string `json:"errortypes"`
}

type Nodes_Reg struct {
	UserId    string `json:"userId"`
	Nodesdata string `json:"Nodesdata"`
	Nodetoken string `json:"nodetoken"`
	NodeIP    string `json:"nodeip"`
	Nodeport  string `json:"nodeport"`
	Speed     string `json:"speed"`
	Nodeid    string `json:"nodeid"`
	Order     string `json:"order"`
}

var db *sqlx.DB
var db_nodes *sqlx.DB
var cfg *ini.File
var f *os.File
var (
	WarningLog *log.Logger
	InfoLog    *log.Logger
	ErrorLog   *log.Logger
)

func init() {

	logfile := time.Now().Format("2006-01-02") + "--" + time.Now().Format("15-04-05") + ".log"
	f, err := os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal("文件创建失败")
		return
	}

	cfg, err := ini.Load("config.ini")
	if err != nil {
		log.Fatal("配置文件读取错误!", err)
	}
	multiWriter := io.MultiWriter(os.Stdout, f)
	if fmt.Sprint(cfg.Section("Log").Key("logtype")) == "full" {
		InfoLog = log.New(multiWriter, "[Info] ", log.Ldate|log.Ltime|log.Llongfile)
		WarningLog = log.New(multiWriter, "[Warning] ", log.Ldate|log.Lmicroseconds|log.Llongfile)
		ErrorLog = log.New(multiWriter, "[Error] ", log.Ldate|log.Lmicroseconds|log.Llongfile)
	} else {
		InfoLog = log.New(multiWriter, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)
		WarningLog = log.New(multiWriter, "[Warning] ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
		ErrorLog = log.New(multiWriter, "[Error] ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	}
	if err != nil {
		ErrorLog.Fatal("配置文件读取错误!", err)
	}
	var mysqlcomm = fmt.Sprint(cfg.Section("Database").Key("username"), ":", cfg.Section("Database").Key("password"), "@tcp(", cfg.Section("Database").Key("host"), ":", cfg.Section("Database").Key("port"), ")/", cfg.Section("Database").Key("databasename"))
	Database, dberr := sqlx.Open("mysql", mysqlcomm)
	if dberr != nil {
		ErrorLog.Fatal("\n数据库连接失败!", err)
	}
	db = Database

	db.SetMaxOpenConns(16000)
	db.SetMaxIdleConns(8000)
	db.SetConnMaxLifetime(time.Minute * 10)
	db.Ping()
}

func main() {
	defer f.Close()
	cfg, err := ini.Load("config.ini")
	if err != nil {
		ErrorLog.Fatal("配置文件读取错误!", err)
	}

	host := fmt.Sprintf("%s:%s", cfg.Section("Server").Key("ip"), cfg.Section("Server").Key("port"))

	listener, err := net.Listen("tcp", host)
	if err != nil {
		ErrorLog.Fatal("服务器启动失败", err)
	}
	defer listener.Close()
	InfoLog.Println("Server startup")
	InfoLog.Println("listener", host)
	for {
		// 监听客户端连接请求
		//fmt.Println("服务器等待客户端连接")
		conn, err := listener.Accept()
		if err != nil {
			ErrorLog.Println("listener.Accept err", err)
			return
		}
		// 具体完成服务器和客户端的数据通信
		go HandlerConnect(conn)
	}

}
func HandlerConnect(conn net.Conn) {
	defer conn.Close()
	// 获取连接的客户端Addr
	//addr := conn.RemoteAddr()

	//fmt.Println(addr, "客户端成功连接---")

	// 循环读取客户端发送数据
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n == 0 {
			//fmt.Println("服务器检测到客户端已关闭，断开连接")
			// 这里关闭的只是go线程，main线程还是在for循环的
			return
		}
		if err != nil {
			ErrorLog.Println("conn Read err", err)
			return
		}

		var userdata Userdata_json
		userdata = JsonDeserialize([]byte(buf[:n]))
		switch userdata.Optype {
		case "login":
			userdata_login := LoginUser(userdata)
			B, _ := json.Marshal(userdata_login)
			conn.Write([]byte(B))
		case "signin":
			userdata_signin := SigninUser(userdata)
			B, _ := json.Marshal(userdata_signin)
			conn.Write([]byte(B))
		case "update":
			userdata_update := UpdateUser(userdata)
			B, _ := json.Marshal(userdata_update)
			conn.Write([]byte(B))
		case "connect":
			var pp Nodes_Connect
			err := json.Unmarshal([]byte(buf[:n]), &pp)
			if err != nil {
				log.Println(err)
			}
			userdata_update := JsonConnect(pp)
			B, _ := json.Marshal(userdata_update)
			conn.Write([]byte(B))
		case "registra":
			var pp Nodes_Reg
			err := json.Unmarshal([]byte(buf[:n]), &pp)
			if err != nil {
				log.Println(err)
			}
			userdata_update := JsonReg(pp)
			B, _ := json.Marshal(userdata_update)
			conn.Write([]byte(B))
		}
	}
}
func LoginUser(userdata Userdata_json) Userdata_json {
	var userdata_back Userdata_json
	stmt, err := db.Prepare("SELECT * FROM frpuesrdata where " + userdata.Type + " = ?")
	if err != nil {
		ErrorLog.Println(err)
	}
	defer stmt.Close()
	var Userid, Username, Telephone, Email, Password, Nodesnum, NodesMaxnum, Member string
	switch userdata.Type {
	case "id":
		err = stmt.QueryRow(userdata.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
	case "email":
		err = stmt.QueryRow(userdata.Email).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
	case "telephone":
		err = stmt.QueryRow(userdata.Telephone).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
	}

	if err != nil {
		if err != sql.ErrNoRows {
			userdata_back.Errortypes = "database Error"
			ErrorLog.Println("database Error", err)
			return userdata_back
		}
	}

	passwordtype := Password == userdata.Password

	stmt, err = db.Prepare("select group_concat(userport.nodedata separator '/'),group_concat(userport.nodeid separator '/')  FROM userport  WHERE userport.userid = ?")
	if err != nil {
		WarningLog.Println(err)
	}
	var nodedata, nodeid string
	err = stmt.QueryRow(Userid).Scan(&nodedata, &nodeid)

	if passwordtype {
		userdata_back.UserId = Userid
		userdata_back.Username = Username
		userdata_back.Telephone = Telephone
		userdata_back.Email = Email
		userdata_back.Password = Password
		userdata_back.Nodesnum = Nodesnum
		userdata_back.NodesMaxNum = NodesMaxnum
		userdata_back.Member = Member
		userdata_back.Nodesdata = nodedata
		userdata_back.Nodesid = nodeid
	} else {
		userdata_back.Errortypes = "password err"
		WarningLog.Printf("login data error")
	}
	defer stmt.Close()
	return userdata_back
}
func SigninUser(userdata Userdata_json) Userdata_json {

	var userdata_ret Userdata_json
	var Userid, Username, Telephone, Email, Password, Nodesnum, NodesMaxnum, Member, Member_in string
	Member_in = "0"

	stmt, err := db.Prepare("SELECT max(userid) from frpuesrdata")
	if err != nil {
		WarningLog.Println(err)
	}
	err = stmt.QueryRow().Scan(&Userid)
	if err != nil {
		if err == sql.ErrNoRows {
			WarningLog.Println(err)
		}
	}
	id, err := strconv.Atoi(Userid)
	id = id + 1
	if err != nil {
		userdata_ret.Errortypes = "DATAERR!!!!!!!"
		defer stmt.Close()
		return userdata_ret
	}
	sqlinsert := "insert into frpuesrdata(userid,username,password,email,telephone,member)values (?,?,?,?,?,?)"
	_, erri := db.Exec(sqlinsert, strconv.Itoa(id), userdata.Username, userdata.Password, userdata.Email, userdata.Telephone, Member_in)
	if erri != nil {
		userdata_ret.Errortypes = "The phone, email, or username has already been utilized"
	}
	stmt, err = db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
	if err != nil {
		WarningLog.Println(err)
	}
	err = stmt.QueryRow(strconv.Itoa(id)).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
	if err != nil {
		if err == sql.ErrNoRows {
			WarningLog.Println(err)
		}
	}

	userdata_ret.UserId = Userid
	userdata_ret.Username = Username
	userdata_ret.Telephone = Telephone
	userdata_ret.Email = Email
	userdata_ret.Password = Password
	userdata_ret.Nodesnum = Nodesnum
	userdata_ret.NodesMaxNum = NodesMaxnum
	userdata_ret.Member = Member
	defer stmt.Close()
	return userdata_ret
}
func UpdateUser(userdata Userdata_json) Userdata_json {
	var userdata_ret Userdata_json
	var Userid, Username, Telephone, Email, Password, Nodesnum, NodesMaxnum, Member string
	switch userdata.Type {
	case "username":
		sqlupdate := fmt.Sprintln("UPDATE frpuesrdata SET username='"+userdata.Username+"' WHERE userid=", userdata.UserId)
		res, err := db.Exec(sqlupdate)
		if err != nil {
			WarningLog.Println("exec failed,", err)
			return userdata_ret
		}
		//查询影响的行数，判断修改插入成功
		row, err := res.RowsAffected()
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}

		stmt, err := db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
		if err != nil {
			WarningLog.Println(err)
		}

		err = stmt.QueryRow(userdata.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
		if err != nil {
			if err == sql.ErrNoRows {
				WarningLog.Println(err)
			}
		}
		if row == 1 {
			userdata_ret.Optype = "Success"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		} else {
			userdata_ret.Optype = "failure"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		}
	case "password":
		sqlupdate := fmt.Sprintln("UPDATE frpuesrdata SET password='"+userdata.Password+"' WHERE userid=", userdata.UserId)
		res, err := db.Exec(sqlupdate)
		if err != nil {
			WarningLog.Println("exec failed,", err)
			return userdata_ret
		}
		//查询影响的行数，判断修改插入成功
		row, err := res.RowsAffected()
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}

		stmt, err := db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
		if err != nil {
			WarningLog.Println(err)
		}

		err = stmt.QueryRow(userdata.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
		if err != nil {
			if err == sql.ErrNoRows {
				WarningLog.Println(err)
			}
		}
		if row == 1 {
			userdata_ret.Optype = "Success"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		} else {
			userdata_ret.Optype = "failure"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		}
	case "telephone":
		sqlupdate := fmt.Sprintln("UPDATE frpuesrdata SET telephone='"+userdata.Telephone+"' WHERE userid=", userdata.UserId)
		res, err := db.Exec(sqlupdate)
		if err != nil {
			fmt.Println("exec failed,", err)
			return userdata_ret
		}
		//查询影响的行数，判断修改插入成功
		row, err := res.RowsAffected()
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}

		stmt, err := db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
		if err != nil {
			WarningLog.Println(err)
		}
		defer stmt.Close()

		err = stmt.QueryRow(userdata.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
		if err != nil {
			if err == sql.ErrNoRows {
				WarningLog.Println(err)
			}
		}
		if row == 1 {
			userdata_ret.Optype = "Success"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		} else {
			userdata_ret.Optype = "failure"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = Nodesnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		}

	case "email":

		sqlupdate := fmt.Sprintln("UPDATE frpuesrdata SET email='"+userdata.Email+"' WHERE userid=", userdata.UserId)
		res, err := db.Exec(sqlupdate)
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}
		//查询影响的行数，判断修改插入成功
		row, err := res.RowsAffected()
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}

		stmt, err := db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}
		defer stmt.Close()

		err = stmt.QueryRow(userdata.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
		if err != nil {
			if err == sql.ErrNoRows {
				WarningLog.Println("rows failed", err)
				return userdata_ret
			}
		}
		if row == 1 {
			userdata_ret.Optype = "Success"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		} else {
			userdata_ret.Optype = "failure"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		}
	case "member":

		sqlupdate := fmt.Sprintln("UPDATE frpuesrdata SET member='"+userdata.Member+"' WHERE userid=", userdata.UserId)
		res, err := db.Exec(sqlupdate)
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}
		//查询影响的行数，判断修改插入成功
		row, err := res.RowsAffected()
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}

		switch Member {
		case "0":
			sqlupdate := fmt.Sprintln("UPDATE frpuesrdata SET nodesnaxnum = '9999999999' WHERE userid = ", userdata.UserId)
			res, err := db.Exec(sqlupdate)
			if err != nil {
				WarningLog.Println("rows failed", err)
				return userdata_ret
			}
			//查询影响的行数，判断修改插入成功
			_, err = res.RowsAffected()
			if err == sql.ErrNoRows {
				WarningLog.Println("rows failed", err)
				return userdata_ret
			}
		}

		stmt, err := db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
		if err != nil {
			WarningLog.Println("rows failed", err)
			return userdata_ret
		}
		defer stmt.Close()

		err = stmt.QueryRow(userdata.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &NodesMaxnum)
		if err != nil {
			if err == sql.ErrNoRows {
				WarningLog.Println("rows failed", err)
				return userdata_ret

			}
		}
		if row == 1 {
			userdata_ret.Optype = "Success"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		} else {
			userdata_ret.Optype = "failure"
			userdata_ret.UserId = Userid
			userdata_ret.Username = Username
			userdata_ret.Telephone = Telephone
			userdata_ret.Email = Email
			userdata_ret.Password = Password
			userdata_ret.Nodesnum = Nodesnum
			userdata_ret.NodesMaxNum = NodesMaxnum
			userdata_ret.Member = Member
			defer stmt.Close()
			return userdata_ret
		}
	}

	return userdata_ret
}
func JsonDeserialize(jsondata []byte) Userdata_json {
	var pp Userdata_json
	err := json.Unmarshal(jsondata, &pp)
	if err != nil {
		WarningLog.Println("unmarshal error")
	}
	return pp
}
func JsonConnect(UserConnectionRequest Nodes_Connect) Nodes_Connect {
	var userdata Nodes_Connect

	stmt, err := db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
	if err != nil {
		ErrorLog.Println(err)
	}
	defer stmt.Close()
	var Userid, Username, Telephone, Email, Password, Nodesnum, Member, nodemaxnam string

	err = stmt.QueryRow(UserConnectionRequest.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &nodemaxnam)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Println(err)
		}
	}

	stmt, err = db.Prepare("SELECT * FROM frpnode where Noderemarks = ?")
	if err != nil {
		ErrorLog.Println(err)
	}
	defer stmt.Close()

	var Nodeid, Noderemarks, nodeip, nodecanuseportmin, nodecanuseportmax, token string

	err = stmt.QueryRow(UserConnectionRequest.Nodesdata).Scan(&Nodeid, &Noderemarks, &nodeip, &nodecanuseportmin, &nodecanuseportmax, &token)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Println(err)
		}
	}
	var port, userid, nodedata, speed string

	stmt, err = db.Prepare("SELECT * FROM userport where nodeid = ? and userid = ? and nodedata = ?")
	if err != nil {
		ErrorLog.Println(err)
	}
	defer stmt.Close()

	err = stmt.QueryRow(Nodeid, UserConnectionRequest.UserId, UserConnectionRequest.Nodeid).Scan(&Nodeid, &userid, &port, &nodedata, &speed)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Println(err)
		}
	}

	userdata.NodeIP = nodeip
	userdata.Nodeport = port
	userdata.Nodetoken = token
	userdata.Nodesdata = UserConnectionRequest.Nodesdata
	userdata.Nodeid = UserConnectionRequest.Nodeid
	//userdata.Type = UserConnectionRequest.Type
	userdata.UserId = UserConnectionRequest.UserId
	userdata.Speed = speed
	defer stmt.Close()
	return userdata
}
func JsonReg(UserRegRequest Nodes_Reg) Nodes_Reg {
	var userdata Nodes_Reg
	stmt, err := db.Prepare("SELECT * FROM frpuesrdata where userid = ?")
	if err != nil {
		ErrorLog.Println(err)
	}
	defer stmt.Close()
	var Userid, Username, Telephone, Email, Password, Member, nodeid, port, nodeip, token string
	var minport, Nodesnum, maxport, maxNodesnum, maxNodesnum_r int

	err = stmt.QueryRow(UserRegRequest.UserId).Scan(&Userid, &Username, &Password, &Email, &Telephone, &Nodesnum, &Member, &maxNodesnum)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Println(err)
		}
	}

	stmt, err = db.Prepare("SELECT nodeid,nodecanuseportmin,nodecanuseportmax,nodeip,token FROM frpnode where noderemarks = ?")
	if err != nil {
		ErrorLog.Println(err)
	}
	defer stmt.Close()

	err = stmt.QueryRow(UserRegRequest.Nodesdata).Scan(&nodeid, &minport, &maxport, &nodeip, &token)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Fatal(err, "aaa")
		}
	}
	Nodeport, err := strconv.Atoi(UserRegRequest.Nodeport)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Println(err)
		}
	}

	stmt, err = db.Prepare("SELECT count(userid = ?) FROM userport")
	if err != nil {
		ErrorLog.Println(err)
	}

	err = stmt.QueryRow(UserRegRequest.UserId).Scan(&maxNodesnum_r)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Fatal(err, "NoRows")
		}
	}

	stmt, err = db.Prepare("SELECT port FROM userport WHERE nodeid = ? and userid = ? and port = ?")
	if err != nil {
		log.Println(err)
	}
	err = stmt.QueryRow(nodeid, UserRegRequest.UserId, UserRegRequest.Nodeport).Scan(&port)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLog.Println(err)
			defer stmt.Close()
			return userdata
		}
	}

	if port == UserRegRequest.Nodeport {
		userdata.Order = "port bin use"
		return userdata
	} else {

		if Nodeport > minport {
			if Nodeport < maxport {
				if maxNodesnum_r >= maxNodesnum {
					userdata.Order = "maxNode"
				} else {
					rand.Seed(time.Now().Unix())
					b := make([]byte, 4)              //随机生成字符数组
					rand.Read(b)                      //整合
					rand_str := hex.EncodeToString(b) //转换为string
					Nodesnum = Nodesnum + 1
					Nodesnum_s := fmt.Sprintf("%04d", Nodesnum)
					Nodesnumstr := rand_str
					switch Member {
					case "1":
					case "2":
					case "0":
						sqlinsert := "insert into userport(nodeid ,userid , port, nodedata, speed )values (?,?,?,?,?)"
						_, err := db.Exec(sqlinsert, nodeid, UserRegRequest.UserId, UserRegRequest.Nodeport, Nodesnumstr, UserRegRequest.Speed)
						if err != nil {
							userdata.Order = "fall"
							ErrorLog.Println(err, "fall")
							return userdata
						}
						//fmt.Println(Nodesnum)
						sqlupdate := fmt.Sprintln("UPDATE frpuesrdata SET Nodesnum='"+Nodesnum_s+"' WHERE userid=", Userid)
						res, err := db.Exec(sqlupdate)
						if err != nil {
							ErrorLog.Println("exec failed,", err)
						}
						//查询影响的行数，判断修改插入成功
						row, err := res.RowsAffected()
						if err != nil {
							ErrorLog.Println("rows failed", err)
						}
						stmt, err = db.Prepare("SELECT userid,port,nodedata,speed FROM userport where userid = ? and nodedata = ?")
						if err != nil {
							ErrorLog.Println(err)
						}
						var userid, port_out, nodeid, speed string
						err = stmt.QueryRow(UserRegRequest.UserId, Nodesnumstr).Scan(&userid, &port_out, &nodeid, &speed)
						if err != nil {
							if err != sql.ErrNoRows {
								ErrorLog.Fatal(err, "aaa")
							}
						}
						defer stmt.Close()
						userdata.UserId = userid
						userdata.NodeIP = nodeip
						userdata.Nodeport = port_out
						userdata.Nodetoken = token
						userdata.Speed = speed
						userdata.Nodeid = nodeid
						userdata.Nodesdata = UserRegRequest.Nodesdata
						userdata.Order = strconv.FormatInt(row, 10)
						return userdata

					}
				}
			}
		}
	}
	defer stmt.Close()
	return userdata
}
