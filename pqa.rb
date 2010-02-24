#!/usr/local/bin/ruby -w

require "pp"

DEFAULT_TOP=10
BUG_URL_STRING="This is a <a href=\"http://pgfoundry.org/tracker/?atid=130&group_id=1000008&func=browse\">bug</a>."

class PQA
  def PQA.pqa_usage
    puts "=============================="
    puts "Usage: " + $0 + " [-logtype syslog|pglog|mysql] [-top n] [-normalize] [-format text|html] [-reports rep1,rep2,...,repn] -file log_file_name"
    puts "Report types : overall, bytype, mosttime, slowest, mostfrequent, errors"
    puts "For example:"
    puts "ruby pqa.rb -logtype pglog -top 10 -normalize -format text -reports overall,slowest ../sample/pglog_sample.log"
    puts "=============================="
  end
  def PQA.run
    (PQA.pqa_usage ; exit) if ARGV == nil 
    if !ARGV.include?("-file")
    puts "=============================="
      puts "## No log file specified; use the '-file' parameter"
      pqa_usage ; exit
    end
    log = nil
    if ARGV.include?("-logtype") && ARGV[ARGV.index("-logtype")+1] == "syslog"
      log = GenericLogReader.new(ARGV[ARGV.index("-file")+1], "SyslogPGParser", "PostgreSQLAccumulator")
    elsif ARGV.include?("-logtype") && ARGV[ARGV.index("-logtype")+1] == "mysql"
      log = GenericLogReader.new(ARGV[ARGV.index("-file")+1], "MySQLLogLine", "MySQLAccumulator")
    else
      log = GenericLogReader.new(ARGV[ARGV.index("-file")+1], "PostgresLogParser", "PostgreSQLAccumulator")
    end
    log.parse
    log.normalize if ARGV.include?("-normalize")  
    top = (ARGV.include?("-top") ? ARGV[ARGV.index("-top")+1] : DEFAULT_TOP).to_i
    format = (ARGV.include?("-format") ? ARGV[ARGV.index("-format")+1] : "text")
    databases = (ARGV.include?("-db") ? (ARGV[ARGV.index("-db")+1]).split(",") : ["*"])

    if databases == ["all"] then
      log.queries.each{|query| databases.push(query.db) if ! databases.index(query.db)}
    end

    if databases == ["list"] then
      databases = []
      log.queries.each{|query| databases.push(query.db) if ! databases.index(query.db)}
      databases.each{|db| puts db}
      Process.exit
    end

    rpts = []

    databases.each{|database|
      queries_keep = Array.new(log.queries)
      if database != "*"
        puts database
        if database != "all"
          log.queries.delete_if{|query| return_val = query.db != database; return_val}
        end
      end
      
      if ! log.queries.empty?

        if ARGV.include?("-reports")
          reports_array = ARGV[ARGV.index("-reports")+1].split(',')
          rpts.push(OverallStatsReport.new(log)) if reports_array.include?("overall")
          rpts.push(QueriesByTypeReport.new(log)) if reports_array.include?("bytype")
          rpts.push(QueriesThatTookUpTheMostTimeReport.new(log,top)) if reports_array.include?("mosttime")
          rpts.push(SlowestQueriesReport.new(log, top)) if reports_array.include?("slowest")
          rpts.push(MostFrequentQueriesReport.new(log, top)) if reports_array.include?("mostfrequent")
          rpts.push(ErrorReport.new(log)) if reports_array.include?("errors")
          #rpts.push() if reports_array.include?("")
          rpts.push(ParseErrorReport.new(log))
        else
          rpts = [OverallStatsReport.new(log), QueriesByTypeReport.new(log), QueriesThatTookUpTheMostTimeReport.new(log, top), SlowestQueriesReport.new(log, top), MostFrequentQueriesReport.new(log, top), ParseErrorReport.new(log)] 
        end
        report_aggregator = (format == "text") ? TextReportAggregator.new : HTMLReportAggregator.new
        puts report_aggregator.create(rpts)
      end
      log.queries = queries_keep
    }
  end
end

# Log file parsers
class ParseError  
  attr_reader :exception, :line
  def initialize(e, line)  
    @exception = e
    @line = line
  end
end

class GenericLogReader
  DEBUG = false
  attr_accessor :includes_duration, :queries, :errors, :parse_errors
  attr_reader :time_to_parse

  def initialize(filename, line_parser_name, accumulator_name)
    @filename = filename
    @line_parser_name = line_parser_name
    @accumulator_name = accumulator_name
    @includes_duration = false
    @queries, @errors , @parse_errors= [], [], []
  end

  def parse
    start = Time.new
    a = Object.const_get(@accumulator_name).new
    puts "Using #{@accumulator_name}" if DEBUG
    p = Object.const_get(@line_parser_name).new
    puts "Using #{@line_parser_name}" if DEBUG
    File.foreach(@filename) {|text|
      begin 
        line = p.parse(text)
        if line
          a.append(line)
        else
          # text.gsub!(/\n/, '\n').gsub!(/\t/, '\t')
          # $stderr.puts "Unrecognized text: '#{text}'"
        end
      rescue StandardError => e
        @parse_errors << ParseError.new(e,line)
      end
    }
    @time_to_parse = Time.new - start
    a.close_out_all
    @queries = a.queries
    @errors = a.errors
    @includes_duration = a.has_duration_info
  end

  def normalize
    @queries.each {|q| q.normalize }
  end

  def unique_queries
    uniq = []
    @queries.each {|x| uniq << x.text if !uniq.include?(x.text) }
    uniq.size
  end
end

#
# MySQL Parsing is broken
#

class MySQLLogLine
  DISCARD = Regexp.new("(^Time )|(^Tcp)|( Quit )|( USE )|(Connect)")
  START_QUERY = Regexp.new('\d{1,5} Query')
  attr_reader :text, :is_new_query, :recognized

  def initialize(text)
    @recognized = true
    @is_new_query = false
    if DISCARD.match(text) != nil
      @recognized = false
      return
    end
    @text = text
    @is_new_query = START_QUERY.match(@text) != nil

  end

  def is_continuation
    @recognized && !/^(\d{1,6})|(\s*)/.match(@text).nil?
  end  

  def is_duration_line
    false
  end  

  def parse_query_segment
    if @is_new_query
      tmp = START_QUERY.match(@text.strip)
      raise StandardError.new("PQA identified a line as the start of a new query, but then was unable to match it with the START_QUERY Regex. #{BUG_URL_STRING}") if tmp == nil
      return tmp.post_match.strip
    end
    @text.strip.chomp
  end

  def to_s
    @text
  end
end

class MySQLAccumulator
  attr_reader :queries

  def initialize
    @current = nil
    @queries = []
  end

  def new_query_start(line)
    @queries << @current if !@current.nil?
    @current = Query.new(line.parse_query_segment)
  end

  def query_continuation(line)
    @current.append(line.parse_query_segment) if !@current.nil?
  end

  def close_out_all ; end
end

#
# PostgreSQL lines Classes
#



class PGLogLine
  DEBUG = false
  attr_accessor :connection_id, :cmd_no, :line_no
  attr_reader :text, :duration, :ignore

  def initialize(text = "NO TEXT", duration = nil)
    @text = text.chomp
    @duration = duration

    if text.nil?
      $stderr.puts "Nil text for line text !" if DEBUG
    end

    # for tracking
    @connection_id = nil
    @cmd_no = nil
    @line_no = nil
  end

  def to_s
    @text
  end

  def parse_duration(time_str, unit)
    unit == "ms" ? (time_str.to_f / 1000.0) : time_str.to_f
  end

  def dump
    self.class.to_s + "(" + @connection_id.to_s + "): " +  text
  end
end

class PGQueryStarter < PGLogLine
  attr_reader :ignore

  def initialize(text, duration = nil, database_name = "UNKNOWN")
    super(filter_query(text), duration)
    @db = database_name
  end

  def filter_query(text)
    @ignore =  (text =~ /begin/i) || (text =~ /VACUUM/i) || (text =~ /^select 1$/i)
    return text
  end

  def append_to(queries)
    query = Query.new(@text, @ignore)
    queries.push(query)
    query.set_db(@db)
    return nil
  end

end

class PGQueryStarterWithDuration < PGQueryStarter
  ignore = false

  def initialize(text, time_str, unit, database_name)
    @time_str = time_str
    @unit = unit
    text_match = /[\s]*(query|statement):[\s]*/i.match(text)
    if text_match
      super(text_match.post_match, parse_duration(time_str, unit), database_name)
    else
      $stderr.puts "Found garbage after Duration line : #{text}"
      super(text, parse_duration(time_str, unit), database_name)
    end
  end

  def append_to(queries)
    queries.got_duration!
    closed_query = queries.pop
    query = Query.new(@text, @ignore)
    query.duration = @duration
    queries.push(query)
    query.set_db(@db)
    return closed_query
  end

end

class PGContinuationLine < PGLogLine
  ignore = false

  def initialize(text, duration = nil)
    super(text.gsub(/\^I/, "\t"))
  end

  def append_to(queries)
    if queries.last.nil?
      # $stderr.puts "Continuation for no previous query (#{@text})"
    else
      queries.last.append(@text)
    end
    return nil
  end

end
  
# Durations

class PGDurationLine < PGLogLine
  ignore = false

  def initialize(time_str, unit)
    @time_str = time_str
    @unit = unit
    super("NO TEXT", parse_duration(time_str, unit))
  end

  def append_to(queries)
    if queries.last.nil?
      # $stderr.puts "Duration for no previous query"
      return nil
    else
      queries.got_duration!
      queries.last.duration = @duration
      return queries.pop
    end
  end

end

# Error Management
# Those 4 classes are untested
# keep ignore = true for the moment

class PGErrorLine < PGLogLine
  ignore = false

  def append_to(errors)
    closed_query = errors.pop
    errors.push(ErrorQuery.new(@text))
    return closed_query
  end

end

class PGHintLine < PGLogLine
  ignore = false

  def append_to(errors)
    if errors.last
      errors.last.append_hint(@text)
    else
      $stderr.puts "Hint for no previous error"
    end
    return nil
  end

end

class PGDetailLine < PGLogLine
  ignore = false

  def append_to(errors)
    if errors.last
      errors.last.append_detail(@text)
    else
      $stderr.puts "Detail for no previous error"
    end
    return nil
  end

end

class PGStatementLine < PGLogLine
  ignore = false

  def append_to(errors)
    if errors.last
      errors.last.append_statement(@text)
    else
      $stderr.puts "Detail for no previous error"
    end
    return nil
  end

end

# Contexts

class PGContextLine < PGLogLine
  ignore = false

  SQL_STATEMENT = /^SQL statement "/
  SQL_FUNCTION = /([^\s]+)[\s]+function[\s]+"([^"]+)"(.*)$/

  def initialize(text)
    statement_match = SQL_STATEMENT.match(text)
    if statement_match
      super(statement_match.post_match[0..-1])
    else
      function_match = SQL_FUNCTION.match(text)
      if function_match
        super(function_match[2])
      else
        $stderr.puts "Unrecognized Context" if DEBUG
        super(text)
      end
      @match_all = true
    end
  end

  def append_to(queries)
    sub_query = queries.pop
    if sub_query.nil?
      $stderr.puts "Missing Query for Context"
    elsif queries.last
      queries.last.set_subquery(sub_query.to_s)
    else
      $stderr.puts "Context for no previous Query"
    end
    return nil
  end

end

# Statuses
# This class is untested
# please keep ignore = true for the moment

class PGStatusLine < PGLogLine
  ignore = true
  CONN_RECV = /connection received: host=([^\s]+) port=([\d]+)/
  CONN_AUTH = /connection authorized: user=([^\s]+) database=([^\s]+)/

  def append_to(stream)
    conn_recv = CONN_RECV.match(@text)
    if conn_recv
      stream.set_host_conn!(conn_recv[1], conn_recv[2])
    end
  
    conn_auth = CONN_AUTH.match(@text)
    if conn_auth
      stream.set_user_db!(conn_auth[1], conn_auth[2])
    end
    return nil  
  end

end
  

  
class PostgreSQLParser
  LOG_LINE_PREFIX = Regexp.new("^(.*)LOG:")
  DATABASE_NAME = Regexp.new('.*database_name:[\s]*([^\s]*)')
  LOG_OR_DEBUG_LINE = Regexp.new("^(LOG|DEBUG):[\s]*")
  QUERY_STARTER = Regexp.new("^(query|statement):[\s]*")
  STATUS = Regexp.new("^(connection|received|unexpected EOF)")
  DURATION = Regexp.new('^duration:([\s\d\.]*)(sec|ms)')
  CONTINUATION_LINE = /^(\^I|\s|\t)/
  CONTEXT_LINE = /^CONTEXT:[\s]*/
  ERROR_LINE = /^(WARNING|ERROR|FATAL|PANIC):[\s]*/
  HINT_LINE = /^HINT:[\s]*/
  DETAIL_LINE = /^DETAIL:[\s]*/
  STATEMENT_LINE = /^STATEMENT:[\s]*/

  # It would be nice to ignore any stuff before the LOG/DEBUG keyword, since a log can have anything in the log line prefix.
  # This should recognize and record the database name.

  def parse(text)
    logprefix_match = LOG_LINE_PREFIX.match(text)
    database_name = "UNKNOWN"
    if logprefix_match
      text = "LOG:" + logprefix_match.post_match
      prefix = logprefix_match[1]
      database_match = DATABASE_NAME.match(prefix)
      if database_match
        database_name = database_match[1]
      end
    end
    logdebug_match = LOG_OR_DEBUG_LINE.match(text)
    if logdebug_match

      query_match = QUERY_STARTER.match(logdebug_match.post_match)
      if query_match
        return PGQueryStarter.new(query_match.post_match, nil, database_name)
      end

      duration_match = DURATION.match(logdebug_match.post_match)
      if duration_match
        additionnal_info = duration_match.post_match.strip.chomp
        if additionnal_info == ""
          return PGDurationLine.new(duration_match[1].strip, duration_match[2])
        else
          return PGQueryStarterWithDuration.new(additionnal_info, duration_match[1].strip, duration_match[2], database_name)
        end
      end

      status_match = STATUS.match(logdebug_match.post_match)
      if status_match
        return PGStatusLine.new(logdebug_match.post_match)
      end

      # $stderr.puts "Unrecognized LOG or DEBUG line: #{text}"
      return nil
    end

    error_match = ERROR_LINE.match(text)
    if error_match
      return PGErrorLine.new(error_match.post_match)
    end

    context_match = CONTEXT_LINE.match(text)
    if context_match
      return PGContextLine.new(context_match.post_match)
    end

    continuation_match = CONTINUATION_LINE.match(text)
    if continuation_match
      return PGContinuationLine.new(continuation_match.post_match)
    end

    statement_match = STATEMENT_LINE.match(text)
    if statement_match
      return PGStatementLine.new(statement_match.post_match)
    end

    hint_match = HINT_LINE.match(text)
    if hint_match
      return PGHintLine.new(hint_match.post_match)
    end

    detail_match = DETAIL_LINE.match(text)
    if detail_match
      return PGDetailLine.new(detail_match.post_match)
    end

    if text.strip.chomp == ""
      return PGContinuationLine.new("")
    end

    # $stderr.puts "Unrecognized PostgreSQL log line: #{text}"
    return nil
  end

end

class SyslogPGParser < PostgreSQLParser
  CMD_LINE = Regexp.new('\[(\d{1,10})(\-\d{1,5}){0,1}\] ')


  def initialize(syslog_str = 'postgres')
    @postgres_pid = Regexp.new(" " + syslog_str + '\[(\d{1,5})\]: ')
  end

  def parse(data)
    recognized = false

    pid_match=@postgres_pid.match(data)
    return if pid_match.nil?

    connection_id = pid_match[1]
    text = pid_match.post_match
    return nil if text == nil

    line_id_match = CMD_LINE.match(text)
    return nil if line_id_match.nil?

    text = line_id_match.post_match
    cmd_no = line_id_match[1]
    if line_id_match[2]
      line_no = line_id_match[2][1..-1]
    else
      line_no = 1
    end

    result = super(text)
    return nil if result.nil?

    result.connection_id = connection_id
    result.cmd_no = cmd_no
    result.line_no = line_no 

     # $stderr.puts result.dump

    return result
  end
end

class PostgresLogParser < PostgreSQLParser
  STARTS_WITH_DATE=Regexp.new(
            '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]([\s]([\d]{2,2}:){1,22}[\d]{2,2} )?[\s]*(PST|[A-Z]{3,3}|-?[\d]{1,2}:?([\d]{2,2})?|z(ulu)?)?[\s]')
  STARTS_WITH_PID=Regexp.new('\[(\d{1,5})\] ')

  def initialize
    @conn_id_found = false
    @last_conn_id = nil
  end

  def parse(text)  
    connection_id = nil
    # text = STARTS_WITH_DATE.match(text) ? text.split(" ")[2..-1].join(" ").strip : text
    date_match = STARTS_WITH_DATE.match(text)
    text = date_match ? date_match.post_match : text
    pid_match = STARTS_WITH_PID.match(text)
    if pid_match
      @conn_id_found = true
      connection_id = pid_match[1]
      @last_conn_id = connection_id
      text = pid_match.post_match.strip
    end
    
    result = super(text)
    # Badly formated continuations need this...
    #if result.nil?
    #  result = PGContinuationLine.new(text)
    #end
    return nil if result.nil?

    if pid_match
      result.connection_id = connection_id
    else
      result.connection_id = @last_conn_id
    end

    # $stderr.puts result.dump

    return result
  end
end

class LogStream
  attr_reader  :has_duration_info
  def initialize
    @queries = []
    @has_duration_info = false

    @host = "UNKNOWN" 
    @port = "UNKNOWN"
    @user = "UNKNOWN"
    @db   = "UNKNOWN"
  end

  def queries
    @queries.reject {|q| q.ignored}
  end 

  def append(line)
    return line.append_to(self)
  end

  def push(query)
    query.set_db(@db)
    query.set_user(@user)
    @queries.push(query)
  end

  def pop
    @queries.pop
  end  

  def last
    @queries.last
  end

  def set_host_conn!(host, port)
    @host = host
    @port = port
  end

  def set_user_db!(user, db)
    @user = user
    @db = db
  end

  def got_duration!
    @has_duration_info = true
  end
end


class PostgreSQLAccumulator
  attr_reader :queries, :errors, :has_duration_info

  def initialize
    @queries = []
    @errors = []
    @working = {}
    @stream = LogStream.new
    @has_duration_info = false
  end

  def append(line)
    if line.connection_id
      if !@working.has_key?(line.connection_id)
        @working[line.connection_id] = LogStream.new
      end
      query = @working[line.connection_id].append(line)
    else
      # no pid mode :
      query = @stream.append(line)
    end
    if query && !query.ignored
      query.accumulate_to(self)
    end
  end

  def append_query(query)
    @queries.push(query)
  end

  def append_error(error)
    @errors.push(error)
  end

  def close_out_all 
    @stream.queries.each { |q| q.accumulate_to(self) }
    @has_duration_info = @stream.has_duration_info
    @working.each {|k, stream|
      stream.queries.each { |q| q.accumulate_to(self) }
      @has_duration_info = @has_duration_info || stream.has_duration_info
    }
  end
end

class Query
  DEBUG = false
  REMOVE_TEXT = Regexp.new("'[^']*'")
  REMOVE_NUMBERS = Regexp.new('([^a-zA-Z_\$])([0-9]{1,10})')

  attr_reader :text, :db, :user
  attr_accessor :duration, :ignored, :q_id

  def initialize(text="", ignored=false)
    $stderr.puts "NIL txt for Query" if text.nil? && DEBUG
    @text = text
    @duration = nil
    @subqueries = []
    @parsing_subs = false
    @ignored = ignored
    @normalized = false
  end

  def append(txt)  
    $stderr.puts "NIL txt for append" if txt.nil? && DEBUG
    if @parsing_subs
      @subqueries.last << " " << txt
    else
      @text << " " << txt
    end
  end

  def set_subquery(text)
    $stderr.puts "NIL txt for sub_q" if text.nil? && DEBUG
    @parsing_subs = true
    @subqueries << text
  end

  def set_db(db)
    @db = db
  end

  def set_user(user)
    @user = user
  end

  def normalize
    if @text
      @text.gsub!(/\\'/, '')
      @text.gsub!(REMOVE_TEXT, "{ }")
      @text.gsub!(REMOVE_NUMBERS, '\1{ }')
      @text.squeeze!(" ")  
      @text.strip!
    end
    @normalized = true
    @text
  end

  def accumulate_to(accumulator)
    accumulator.append_query(self)
  end
#
# Does not work for the moment
#
#  def text
#    if @normalized
#      @text
#    else
#      "[" + @db + "," + @user + "] " + @text
#    end
#  end
#  def to_s
#    text
#  end
  def to_s
    @text
  end

  def is_select
    check(/^SELECT/i)
  end

  def is_delete
    check(/^DELETE/i)
  end

  def is_insert
    check(/^INSERT/i)
  end

  def is_update
    check(/^UPDATE/i)
  end

  def check(regexp)
    regexp.match(@text.strip) != nil
  end
end

# Errors not used for the moment

class ErrorQuery < Query
  attr_reader :text, :hint, :detail, :error

  is_select = false
  is_delete = false
  is_insert = false
  is_update = false

  def initialize(text="NO ERROR MESSAGE")
    @error = text
    @hint = ''
    @detail = ''
    super("NO STATEMENT")
  end

  def append_statement(text)
    $stderr.puts "NIL txt for error statement" if text.nil? && DEBUG
    @text=text
  end

  def append_hint(text)
    $stderr.puts "NIL txt for error hint" if text.nil? && DEBUG
    @hint = text
  end

  def append_detail(text)
    $stderr.puts "NIL txt for error detail" if text.nil? && DEBUG
    @detail = text
  end

  def accumulate_to(accumulator)
    accumulator.append_error(self)
  end

end

# Reports 
class TextReportAggregator
  def create(reports)  
    rpt = ""
    reports.each {|r| 
      next if !r.applicable
      rpt << r.text 
    }
    rpt
  end
end

class HTMLReportAggregator
  def create(reports) 
    rpt = "<html><head>"
    rpt =<<EOS
<style type="text/css">
body { background-color:white; }
h2 { text-align:center; }
h3 { color:blue }
p, td, th { font-family:Courier, Arial, Helvetica, sans-serif; font-size:14px; }
th { color:white; background-color:#7B8CBE; }
span.keyword { color:blue; }
</style>
EOS
#tr { background-color:#E1E8FD; }
    rpt << "<title>SQL Query Analysis (generated #{Time.now})</title></head><body>\n"
    rpt << "<h2>SQL Query Analysis (generated #{Time.now})</h2><br>\n"
    rpt << "<hr><center>"
    rpt << "<table><th>Reports</th>"
    reports.each_index {|x| 
      next if !reports[x].applicable
      link = "<a href=\"#report#{x}\">#{reports[x].title}</a>"
      rpt << "<tr><td>#{link}</td></tr>"
    }
    rpt << "</table>"
    rpt << "<hr></center>"
    reports.each_index {|x| 
      next if !reports[x].applicable
      rpt << "<a name=\"report#{x}\"> </a>"
      rpt << reports[x].html 
    }
    rpt << "</body></html>\n"
  end
end

class GenericReport

  def initialize(log)
    @log = log 
  end

  def colorize(txt)
    ["SELECT","UPDATE","INSERT INTO","DELETE","WHERE","VALUES","FROM","AND","ORDER BY","GROUP BY","LIMIT", "OFFSET", "DESC","ASC","AS","EXPLAIN","DROP","EXEC"].each {|w| 
      txt = txt.gsub(Regexp.new(w), "<span class='keyword'>#{w}</span>")
    }
    ["select","update","from","where","explain","drop"].each {|w| 
      txt = txt.gsub(Regexp.new(w), "<span class='keyword'>#{w}</span>")
    }
    txt
  end

  def title  
    "Unnamed report"
  end

  def pctg_of(a,b)
    a > 0 ? (((a.to_f/b.to_f)*100.0).round)/100.0 : 0
  end

  def round(x, places)
    (x * 10.0 * places).round / (10.0 * places)
  end

  def applicable
    true
  end
end

class OverallStatsReport < GenericReport

  def html
    rpt = "<h3>#{title}</h3>\n"
    rpt << "#{@log.queries.size} queries\n"
    rpt << "<br>#{@log.unique_queries} unique queries\n"
    if @log.includes_duration
      rpt << "<br>Total query duration was #{round(total_duration, 2)} seconds\n"
      longest = find_longest
      rpt << "<br>Longest query (#{colorize(longest.text)}) ran in #{"%2.3f" % longest.duration} seconds\n"
      shortest = find_shortest
      rpt << "<br>Shortest query (#{colorize(shortest.text)}) ran in #{"%2.3f" % shortest.duration} seconds\n"
    end
    rpt << "<br>Log file parsed in #{"%2.1f" % @log.time_to_parse} seconds\n"
  end

  def title  
    "Overall statistics"
  end

  def text
    rpt = "######## #{title}\n"
    rpt << "#{@log.queries.size} queries (#{@log.unique_queries} unique)"
    rpt << ", longest ran in #{find_longest.duration} seconds)," if @log.includes_duration
    rpt << " parsed in #{@log.time_to_parse} seconds\n"
  end

  def total_duration
    @log.queries.inject(0) {|sum, q| sum += (q.duration != nil) ? q.duration : 0  }
  end

  def find_shortest  
    q = Query.new("No queries found")
    @log.queries.min {|a,b| 
      return b if a.duration.nil?
      return a if b.duration.nil?
      a.duration <=> b.duration 
    }
  end

  def find_longest  
    q = Query.new("No queries found")
    @log.queries.max {|a,b| 
      return b if a.duration.nil?
      return a if b.duration.nil?
      a.duration <=> b.duration 
    }
  end
end

class MostFrequentQueriesReport < GenericReport

  def initialize(log, top=DEFAULT_TOP)
    super(log)
    @top = top
  end

  def title  
    "Most frequent queries"
  end

  def html
    list = create_report
    rpt = "<h3>#{title}</h3>\n"
    rpt << "<table><tr><th>Rank</th><th>Times executed</th><th>Query text</th>\n"
    (list.size < @top ? list.size : @top).times {|x| 
        rpt << "<tr><td>#{x+1}</td><td>#{list[x][1]}</td><td>#{colorize(list[x][0])}</td></tr>\n" 
    }
    rpt << "</table>\n"
  end

  def text
    list = create_report
    rpt = "######## #{title}\n"
    (list.size < @top ? list.size : @top).times {|x| 
        rpt << list[x][1].to_s + " times: " + list[x][0].to_s + "\n" 
    }
    rpt
  end

  def create_report
    h = {}
    @log.queries.each {|q|
      h[q.text] = 0 if !h.has_key?(q.text)
      h[q.text] += 1
    }
    h.sort {|a,b| b[1] <=> a[1] }
  end
end

class LittleWrapper
  attr_accessor :total_duration, :count, :q

  def initialize(q)
    @q = q
    @total_duration = 0.0
    @count = 0
  end

  def add(q)
    return if q.duration.nil?
    @total_duration += q.duration
    @count += 1
  end
end

class QueriesThatTookUpTheMostTimeReport < GenericReport
  def initialize(log, top=DEFAULT_TOP)
    super(log)
    @top = top
  end

  def title  
    "Queries that took up the most time"
  end

  def applicable
    @log.includes_duration
  end

  def html
    list = create_report
    rpt = "<h3>#{title}</h3>\n"
    rpt << "<table><tr><th>Rank</th><th>Total time (seconds)</th><th>Times executed</th><th>Query text</th>\n"
    (list.size < @top ? list.size : @top).times {|x| 
        rpt << "<tr><td>#{x+1}</td><td>#{"%2.3f" % list[x][1].total_duration}</td><td align=right>#{list[x][1].count}</td><td>#{colorize(list[x][0])}</td></tr>\n" 
    }
    rpt << "</table>\n"
  end

  def text
    list = create_report
    rpt = "######## #{title}\n"
    (list.size < @top ? list.size : @top).times {|x| 
        rpt << "#{"%2.3f" % list[x][1].total_duration} seconds: #{list[x][0]}\n" 
    }
    rpt
  end

  def create_report
    h = {}
    @log.queries.each {|q|
      next if q.duration.nil?
      h[q.text] = LittleWrapper.new(q) if !h.has_key?(q.text)
      h[q.text].add(q)
    }
    h.sort {|a,b| b[1].total_duration <=> a[1].total_duration }
  end
end

class SlowestQueriesReport < GenericReport

  def initialize(log, top=DEFAULT_TOP)
    super(log)
    @top = top
  end

  def applicable
    @log.includes_duration
  end

  def title  
    "Slowest queries"
  end

  def text
    list = create_report
    rpt = "######## #{title}\n"
    (list.size < @top ? list.size : @top).times {|x| 
        rpt << "#{"%2.3f" % list[x].duration} seconds: #{list[x].text}\n" 
    }
    rpt
  end

  def html
    list = create_report
    rpt = "<h3>#{title}</h3>\n"
    rpt << "<table><tr><th>Rank</th><th>Time</th><th>Query text</th>\n"
    (list.size < @top ? list.size : @top).times {|x| 
        rpt << "<tr><td>#{x+1}</td><td>#{"%2.3f" % list[x].duration}</td><td>#{colorize(list[x].text)}</td></tr>\n" 
    }
    rpt << "</table>\n"
  end
  
  def create_report
    (@log.queries.reject{|q| q.duration.nil?}).sort {|a,b| b.duration.to_f <=> a.duration.to_f }.slice(0,@top)
  end
end

class ParseErrorReport < GenericReport

  def title  
    "Parse Errors"
  end

  def applicable
    !@log.parse_errors.empty?
  end

  def text
    rpt = "######## #{title}\n"
    @log.parse_errors.each {|x| rpt << "#{x.exception} : #{x.line}\n" }
    rpt
  end

  def html
    rpt = "<h3>#{title}</h3>\n"
    rpt << "<table><tr><th>Explanation</th><th>Offending line</th>\n"
    @log.parse_errors.each {|x|
        rpt << "<tr><td>#{x.exception.message}</td><td>#{x.line}</td></tr>\n" 
    }
    rpt << "</table>\n"
  end
end

class ErrorReport < GenericReport

  def title  
    "Errors"
  end

  def applicable
    !@log.errors.empty?
  end

  def text
    rpt = "######## #{title}\n"
    @log.errors.each {|x| rpt << "#{x.error} : #{x.text}\n" }
    rpt
  end

  def html
    rpt = "<h3>#{title}</h3>\n"
    rpt << "<table><tr><th>Error</th><th>Offending query</th>\n"
    @log.errors.each {|x|
        message = "<p>#{x.error}</p>" + (x.detail.size > 0 ? "<p>DETAIL : #{x.detail}</p>" : '') + \
               (x.hint.size > 0 ? "<p>HINT : #{x.hint}</p>" : '')
        rpt << "<tr><td>#{message}</td><td>#{colorize(x.text)}</td></tr>\n" 
    }
    rpt << "</table>\n"
  end
end

class QueriesByTypeReport < GenericReport

  def title  
    "Queries by type"
  end

  def html
    sel,ins,upd,del=create_report
    rpt = "<h3>#{title}</h3>\n"
    rpt << "<table><tr><th>Type</th><th>Count</th><th>Percentage</th>\n"
    rpt << "<tr><td>SELECT</td><td>#{sel}</td><td align=center>#{(pctg_of(sel, @log.queries.size)*100).to_i}</td></tr>\n" if sel > 0
    rpt << "<tr><td>INSERT</td><td>#{ins}</td><td align=center>#{(pctg_of(ins, @log.queries.size)*100).to_i}</td></tr>\n" if ins > 0
    rpt << "<tr><td>UPDATE</td><td>#{upd}</td><td align=center>#{(pctg_of(upd, @log.queries.size)*100).to_i}</td></tr>\n" if upd > 0
    rpt << "<tr><td>DELETE</td><td>#{del}</td><td align=center>#{(pctg_of(del, @log.queries.size)*100).to_i}</td></tr>\n" if del > 0
    rpt << "</table>\n"
  end

  def text
    sel,ins,upd,del=create_report
    rpt = "######## #{title}\n"
    rpt << "SELECTs: #{sel.to_s.ljust(sel.to_s.size + 1)} (#{(pctg_of(sel, @log.queries.size)*100).to_i}%)\n" if sel > 0
    rpt << "INSERTs: #{ins.to_s.ljust(sel.to_s.size + 1)} (#{(pctg_of(ins, @log.queries.size)*100).to_i}%)\n" if ins > 0
    rpt << "UPDATEs: #{upd.to_s.ljust(upd.to_s.size + 1)} (#{(pctg_of(upd, @log.queries.size)*100).to_i}%)\n" if upd > 0
    rpt << "DELETEs: #{del.to_s.ljust(sel.to_s.size + 1)} (#{(pctg_of(del, @log.queries.size)*100).to_i}%)\n" if del > 0
    rpt
  end

  def create_report
    [@log.queries.find_all {|q| q.is_select}.size,
    @log.queries.find_all {|q| q.is_insert}.size,
    @log.queries.find_all {|q| q.is_update}.size,
    @log.queries.find_all {|q| q.is_delete}.size]
  end
end

PQA.run if __FILE__ == $0
