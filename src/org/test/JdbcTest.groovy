import java.sql.ResultSet
import java.sql.SQLException

import org.springframework.context.ApplicationContext
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor

ApplicationContext ctx = binding.variables.get('applicationContext')
//def dataSource = ctx.getBean('configDataSource')

def dataSource = new org.apache.commons.dbcp.BasicDataSource()
dataSource.url = 'jdbc:google:mysql://gambino-apps:us-central1:gambino-apps/slots'
dataSource.driverClassName = 'com.mysql.jdbc.GoogleDriver'
dataSource.username = 'serverside'
dataSource.password = 'S1y-)DB!^1J0'

def dataSource1 = new org.apache.commons.dbcp.BasicDataSource()
dataSource1.url = 'jdbc:google:mysql://gambino-apps:slots/slots'
dataSource1.driverClassName = 'com.mysql.jdbc.GoogleDriver'
dataSource1.username = 'appengine1'
dataSource1.password = 'alpha991234noticeLt21'


JdbcTemplate template = new JdbcTemplate(dataSource)
JdbcTemplate template1 = new JdbcTemplate(dataSource1)



def res = template1.query('select id, message from bo_pushes', new ResultSetExtractor() {
    Object extractData(ResultSet rs) throws SQLException ,DataAccessException {
        def res = [:]
        while (rs.next()) {
            res << [(rs.getInt(1)):rs.getString(2)]
        }
        return res
    };
})


//template.execute('select 1')

"OK ${res}".toString()
