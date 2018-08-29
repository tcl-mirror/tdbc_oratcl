
package require Tcl 8.6
package require tdbc
package require Oratcl

package provide tdbc::oratcl 0.1


::namespace eval ::tdbc::oratcl {
  namespace export connection
}


##
# @brief Class representing a connection to a Oratcl database connector
# @class ::tdbc::oratcl::connection
#
::oo::class create ::tdbc::oratcl::connection {
  superclass ::tdbc::connection
  variable orahdl

  constructor {connectStr} {
    next
    set orahdl [oralogon $connectStr]
    # puts "orahdl = $orahdl"
  }


  forward statementCreate ::tdbc::oratcl::statement create


  method close {} {
    # puts "connection close"
    oralogoff $orahdl
    return [next]
  }


  method tables {{pattern %}} {
    set sql {
      select OWNER,TABLE_NAME 
        from All_Tables
	where table_name like :table_name
	order by OWNER,TABLE_NAME
    }
    set cursor [oraopen $orahdl]
    orasql $cursor $sql -parseonly
    orabindexec $cursor :table_name $pattern
    set result {}
    array set data {}
    while {[orafetch $cursor -dataarray data] == 0} {
      lappend result $data(TABLE_NAME) [list owner $data(OWNER)]
    }
    oraclose $cursor
    return $result
  }


  method columns {table {pattern %}} {
    set sql {
      select COLUMN_ID,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE
        from ALL_TAB_COLS 
	where TABLE_NAME = :table_name
	  and COLUMN_NAME like :column_name
	order by COLUMN_ID
    }
    set cursor [oraopen $orahdl]
    orasql $cursor $sql -parseonly
    orabindexec $cursor :table_name $table :column_name $pattern
    set result [dict create]
    array set data {}
    while {[orafetch $cursor -dataarray data] == 0} {
      set infos [dict create]
      dict set infos type [string tolower $data(DATA_TYPE)]
      switch $data(DATA_TYPE) {
	VARCHAR2 - NVARCHAR2 {
	  dict set infos type varchar
	}
	LONG {
	  dict set infos type integer
	}
	RAW {
	  dict set infos type varbinary
	}
	{LONG RAW} - BLOB - BFILE - CFILE - CLOB - NCLOB {
	  dict set infos type longvarbinary
	}
	ROWID {
	  dict set infos type binary
	}
      }
      dict set infos precision $data(DATA_PRECISION)
      if {$data(DATA_PRECISION) eq {}} {
        dict set infos precision $data(DATA_LENGTH)
	if {$data(DATA_SCALE) > 0} {
	  dict set infos precision [+ $data(DATA_PRECISION) $data(DATA_SCALE)]
	}
      }
      dict set infos scale $data(DATA_SCALE)
      dict set infos nullable [string equal $data(DATA_SCALE) Y]
      dict set infos oratype $data(DATA_TYPE)
      dict set infos oraid $data(COLUMN_ID)
      dict set result $data(COLUMN_NAME) $infos
    }
    oraclose $cursor
    return $result
  }


  method primarykeys {table} {
    return -code error "Feature is not supported"
  }


  method foreignkeys {args} {
    return -code error "Feature is not supported"
  }


  method begintransaction {} {
  }


  method commit {} {
    oracommit $orahdl
  }


  method rollback {} {
    oraroll $orahdl
  }


  method prepare {sqlCode} {
    set result [next $sqlCode]
    return $result
  }


  method getDBhandle {} {
    return $orahdl
  }
}


##
# @brief Statement class for tdbc::oratcl connection
# @class ::tdbc::oratcl::statement
#
::oo::class create ::tdbc::oratcl::statement {
  superclass ::tdbc::statement
  variable cursor bindVars

  constructor {connection sqlcode} {
    # puts "statement constructor"
    next
    set bindVars {}
    foreach token [::tdbc::tokenize $sqlcode] {
      if {[string index $token 0] in {$ : @}} {
	# puts "token = $token"
	lappend bindVars [string range $token 1 end]
      }
    }
    set cursor [oraopen [$connection getDBhandle]]
    set rc [oraparse $cursor $sqlcode]
    if {$rc != 0} {
      return -code error [list ORACLE error $rc on oraparse $sqlcode]
    }
  }


  forward resultSetCreate ::tdbc::oratcl::resultset create


  method execute {{bindings {}}} {
    if {$bindVars ne {}} {
      set bindList {}
      foreach varName $bindVars {
	if {[dict exists $bindings $varName]} {
	  lappend bindList :$varName [dict get $bindings $varName]
	} else {
	  lappend bindList :$varName {}
	}
      }
      set rc [orabind $cursor {*}$bindList]
      if {$rc != 0} {
        return -code error [list ORACLE errro $rc in orabind $bindList]
      }
    }
    oraexec $cursor
    return [next]
  }


  method params {} {
    set result [dict create]
    foreach varName $bindVars {
      dict set result $varName [dict create direction in type {} precision {} scale {} nullable {}]
    }
    return $result
  }


  method configure {args} {
    puts [list method configure {*}$args ([llength $args] args)]
    set options {
      -longsize -bindsize -nullvalue -fetchrows -lobpsize -longpsize -utfmode -numbsize -datesize
    }
    if {[llength $args] == 0} {
      return [concat $options]
    }
    if {[llength $args] == 1} {
      lassign $args option 
      if {$option in $options} {
        return [oraconfig $cursor [string range $option 1 end]]
      }
      return -code error "Option \"$option\" is not supported"
    }
    if {[llength $args] % 2 == 0} {
      foreach {option value} $args {
	if {$option in $options} {
	  oraconfig $cursor [string range $option 1 end] $value
	} else {
	  return -code error "Option \"$option\" is not supported"
	}
      }
      return
    }
    return -code error "wrong # args, should be \" configure ?-option value?...\""
  }


  method getDBhandle {} {
    return [$connection getDBhandle]
  }


  method getCursor {} {
    return $cursor
  }


  method close {} {
    # puts "statement close"
    oraclose $cursor
    return [next]
  }
}


##
# @brief Result-Set class for tdbc::oratcl connection
# @class ::tdbc::oratcl::resultset
#
::oo::class create ::tdbc::oratcl::resultset {
  superclass ::tdbc::resultset
  variable cursor columns

  constructor {statement args} {
    # puts "resultset $args"
    set cursor [$statement getCursor]
    set columns [oracols $cursor name]
    next
  }


  method columns {} {
    return $columns
  }


  method rowcount {} {
    set cnt [oramsg $cursor rows]
    return $cnt
  }


  method nextlist {varname} {
    upvar $varname data
    if {[orafetch $cursor -datavariable data] == 0} {
      return 1
    }
    return 0
  }


  method nextdict var {
    upvar $varname data
    if {[orafetch $cursor -datavariable data] == 0} {
      set result [dict create]
      foreach column $columns value $data {
        dict set result $column $value
      }
      set data $result
      return 1
    }
    return 0
  }


  method nextresults {} {
    if {[oramsg $cursor rc] == 0} {
      return 1
    }
    return 0
  }


  method close {} {
    # puts "resultset close"
    return [next]
  }
}

