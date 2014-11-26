#!/usr/bin/env python
# Copyright 2014 Boren Zhang <zhangbr@umich.edu>
#
# This file is a revise of the original impala_shell.py to work with 
# BlinkDB.
#
# Original:
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Impala's shell
import cmd
import errno
import getpass
import os
import prettytable
import re
import shlex
import signal
import socket
import sqlparse
import sys
import time

from impala_client import (ImpalaClient, DisconnectedException, QueryStateException,
                           RPCException, TApplicationException)
from impala_shell_config_defaults import impala_shell_defaults
from option_parser import get_option_parser, get_config_from_file
from shell_output import DelimitedOutputFormatter, OutputStream, PrettyOutputFormatter
from subprocess import call

from impala_shell import ImpalaShell, print_to_stderr, VERSION_STRING, WELCOME_STRING, CmdStatus

class BlinkDBShell(ImpalaShell):
  def __init__(self, options):
    ImpalaShell.__init__(self, options);
    
  def do_select(self, args):
    if " within " in args:
      #TODO implement within <time>
      #within should be last in statement
      indexOfWithin = args.find(" within ");
      impalaCompatibleQueryContent = args[:indexOfWithin];
      withinClause = args[indexOfWithin:];
      withinClauses = withinClause.split();
      withinValue = withinClauses[1];
      print "do %s, within %s" % (impalaCompatibleQueryContent, withinValue);
      ImpalaShell.do_select(self, args);
      return;
    if "approx_sum" in args:
      if not self._blinkdb_installed():
        print "Please install blinkdb's function in impala using 'BLINKDBINSTALL <path to folder for udfs>'";
        return;
      column = re.search(r"approx_sum\((.*)\)", args).group(1);
      approxSumIndex = args.find("approx_sum");
      args = args[:approxSumIndex] + "approx_sum(%s, sampleWeight)" % column \
      + args[approxSumIndex + len("approx_sum()") + len(column):];
    if "approx_count" in args:
      if not self._blinkdb_installed():
        print "Please install blinkdb's function in impala using 'BLINKDBINSTALL <path to folder for udfs>'";
        return;
      column = re.search(r"approx_count\((.*)\)", args).group(1);
      approxSumIndex = args.find("approx_count");
      args = args[:approxSumIndex] + "approx_count(%s, sampleWeight)" % column \
      + args[approxSumIndex + len("approx_count()") + len(column):];
    ImpalaShell.do_select(self, args);
    
  def do_create (self, args) :
    if "randomSampleWith" in args and not "function" in args:
      if not self._blinkdb_installed():
        print "Please install blinkdb's function in impala using 'BLINKDBINSTALL <path to folder for udfs>'";
        return;
      # choose an arbitrary value that follows randomSampleWith
      tableName = re.search(r"(from|FROM) (\w+)", args).group(2);
      query = self.imp_client.create_beeswax_query("describe %s" % tableName,
                                                 self.set_query_options)
      columns = self._execute_stmt_and_return_rows(query);
      arbitraryColumnName = columns[0][0];
      #TODO support UNION of multiple samplewith, currently only one 
      #     samplewith is processed.
      sampleWithProbabilityString = re.search(r"randomSampleWith\((0\.\d+)\)", args).group(1);
      sampleWithProbability = float(sampleWithProbabilityString);
      indexOfSampleWith = args.find("randomSampleWith");
      indexOfFrom = args.find(" from ");
      args = args[:indexOfFrom] \
              + ", %f as sampleWeight " % (1/sampleWithProbability) \
              + args[indexOfFrom:indexOfSampleWith] \
              + "randomSampleWith(%f, %s) " % (sampleWithProbability, arbitraryColumnName) \
              + args[indexOfSampleWith + len("randomSampleWith() ") + len(sampleWithProbabilityString):];
    ImpalaShell.do_create(self, args);
    
  def do_blinkdbinstall (self, args) :
    print "Installing blinkdb to current database ";
    createSampleWithQueryTemplate = "function randomSampleWith(double, %s) returns boolean"\
      + " location '%s/libblinkdbudf.so' symbol='randomSamplingUDF'";
    ImpalaShell.do_create(self, createSampleWithQueryTemplate % ("double", args));
    ImpalaShell.do_create(self, createSampleWithQueryTemplate % ("int", args));
    ImpalaShell.do_create(self, createSampleWithQueryTemplate % ("string", args));
    createApproxAvgQueryTemplate = \
      "aggregate function approx_avg({0}) returns string location "\
       + "'{1}/libblinkdbudf.so' init_fn='ApproxAvg{2}Init' update_fn='ApproxAvg{2}Update' "\
       + "merge_fn='ApproxAvg{2}Merge' serialize_fn='ApproxAvg{2}Serialize' finalize_fn='ApproxAvg{2}Finalize'";
    ImpalaShell.do_create(self, createApproxAvgQueryTemplate.format("double", args, "Double"));
    ImpalaShell.do_create(self, createApproxAvgQueryTemplate.format("int", args, "Int"));
    
    createApproxCountQuery = \
      "aggregate function approx_count(int, double) returns string location "\
       + "'{0}/libblinkdbudf.so' init_fn='ApproxCountInit' update_fn='ApproxCountUpdate' "\
       + "merge_fn='ApproxCountMerge' serialize_fn='ApproxCountSerialize' finalize_fn='ApproxCountFinalize'";
    ImpalaShell.do_create(self, createApproxCountQuery.format(args));
    
    createApproxAvgQueryTemplate = \
      "aggregate function approx_sum({0}, double) returns string location "\
       + "'{1}/libblinkdbudf.so' init_fn='ApproxSum{2}Init' update_fn='ApproxSum{2}Update' "\
       + "merge_fn='ApproxSum{2}Merge' serialize_fn='ApproxSum{2}Serialize' finalize_fn='ApproxSum{2}Finalize'";
    ImpalaShell.do_create(self, createApproxAvgQueryTemplate.format("double", args, "Double"));
    ImpalaShell.do_create(self, createApproxAvgQueryTemplate.format("int", args, "Int"));

  def do_blinkdbuninstall (self, args) :
    ImpalaShell.do_drop(self, "function randomSampleWith(DOUBLE, DOUBLE)");
    ImpalaShell.do_drop(self, "function randomSampleWith(DOUBLE, INT)");
    ImpalaShell.do_drop(self, "function randomSampleWith(DOUBLE, STRING)");
    ImpalaShell.do_drop(self, "aggregate function approx_avg(DOUBLE)");
    ImpalaShell.do_drop(self, "aggregate function approx_avg(INT)");
    ImpalaShell.do_drop(self, "aggregate function approx_count(INT, DOUBLE)");
    ImpalaShell.do_drop(self, "aggregate function approx_sum(INT, DOUBLE)");
    ImpalaShell.do_drop(self, "aggregate function approx_sum(DOUBLE, DOUBLE)");

  def _blinkdb_installed(self):
    query = self.imp_client.create_beeswax_query("show functions",
                                                 self.set_query_options)
    functions = self._execute_stmt_and_return_rows(query);
    findBlinkdbFunctions = False;
    for function in functions:
      if function[1].find("randomsamplewith") >= 0:
        findBlinkdbFunctions = True;
        break;
    return findBlinkdbFunctions;
  def _execute_stmt_and_return_rows(self, query, is_insert=False):
    """ The logic of executing any query statement

    The client executes the query and the query_handle is returned immediately,
    even as the client waits for the query to finish executing.

    If the query was not an insert, the results are fetched from the client
    as they are streamed in, through the use of a generator.

    The execution time is printed and the query is closed if it hasn't been already
    
    return result as an array
    """
    returnVal = [];
    try:
      self._print_if_verbose("Query: %s" % (query.query,))
      start_time = time.time()
      self.last_query_handle = self.imp_client.execute_query(query)
      self.query_handle_closed = False
      wait_to_finish = self.imp_client.wait_to_finish(self.last_query_handle)
      # retrieve the error log
      warning_log = self.imp_client.get_warning_log(self.last_query_handle)

      if is_insert:
        num_rows = self.imp_client.close_insert(self.last_query_handle)
      else:
        # impalad does not support the fetching of metadata for certain types of queries.
        if not self.imp_client.expect_result_metadata(query.query):
          # Close the query
          self.imp_client.close_query(self.last_query_handle)
          self.query_handle_closed = True
          return CmdStatus.SUCCESS

        self._format_outputstream()
        # fetch returns a generator
        rows_fetched = self.imp_client.fetch(self.last_query_handle)
        num_rows = 0
        for rows in rows_fetched:
          for row in rows :
            returnVal.append(row);
          num_rows += len(rows)

      end_time = time.time()

      if warning_log:
        self._print_if_verbose(warning_log)
      # print insert when is_insert is true (which is 1)
      # print fetch when is_insert is false (which is 0)
      verb = ["Fetch", "Insert"][is_insert]
      self._print_if_verbose("%sed %d row(s) in %2.2fs" % (verb, num_rows,
                                                               end_time - start_time))

      if not is_insert:
        self.imp_client.close_query(self.last_query_handle, self.query_handle_closed)
      self.query_handle_closed = True

      profile = self.imp_client.get_runtime_profile(self.last_query_handle)
      self.print_runtime_profile(profile)
      return returnVal
    except RPCException, e:
      # could not complete the rpc successfully
      # suppress error if reason is cancellation
      if self._no_cancellation_error(e):
        print_to_stderr(e)
    except QueryStateException, e:
      # an exception occurred while executing the query
      if self._no_cancellation_error(e):
        self.imp_client.close_query(self.last_query_handle, self.query_handle_closed)
        print_to_stderr(e)
    except DisconnectedException, e:
      # the client has lost the connection
      print_to_stderr(e)
      self.imp_client.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    except socket.error, (code, e):
      # if the socket was interrupted, reconnect the connection with the client
      if code == errno.EINTR:
        print ImpalaShell.CANCELLATION_MESSAGE
        self._reconnect_cancellation()
      else:
        print_to_stderr("Socket error %s: %s" % (code, e))
        self.prompt = self.DISCONNECTED_PROMPT
        self.imp_client.connected = False
    except Exception, u:
      # if the exception is unknown, there was possibly an issue with the connection
      # set the shell as disconnected
      print_to_stderr('Unknown Exception : %s' % (u,))
      self.imp_client.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    return CmdStatus.ERROR



#Copied from impala_shell.py
if __name__ == "__main__":

  # pass defaults into option parser
  parser = get_option_parser(impala_shell_defaults)
  options, args = parser.parse_args()
  # use path to file specified by user in config_file option
  user_config = os.path.expanduser(options.config_file);
  # by default, use the .impalarc in the home directory
  config_to_load = impala_shell_defaults.get("config_file")
  # verify user_config, if found
  if os.path.isfile(user_config) and user_config != config_to_load:
    if options.verbose:
      print_to_stderr("Loading in options from config file: %s \n" % user_config)
    # Command line overrides loading ~/.impalarc
    config_to_load = user_config
  elif user_config != config_to_load:
    print_to_stderr('%s not found.\n' % user_config)
    sys.exit(1)

  # default options loaded in from impala_shell_config_defaults.py
  # options defaults overwritten by those in config file
  try:
    impala_shell_defaults.update(get_config_from_file(config_to_load))
  except Exception, e:
    msg = "Unable to read configuration file correctly. Check formatting: %s\n" % e
    print_to_stderr(msg)
    sys.exit(1)

  parser = get_option_parser(impala_shell_defaults)
  options, args = parser.parse_args()

  # Arguments that could not be parsed are stored in args. Print an error and exit.
  if len(args) > 0:
    print_to_stderr('Error, could not parse arguments "%s"' % (' ').join(args))
    parser.print_help()
    sys.exit(1)

  if options.version:
    print VERSION_STRING
    sys.exit(0)

  if options.use_kerberos and options.use_ldap:
    print_to_stderr("Please specify at most one authentication mechanism (-k or -l)")
    sys.exit(1)

  if options.use_kerberos:
    print_to_stderr("Starting Impala Shell using Kerberos authentication")
    print_to_stderr("Using service name '%s'" % options.kerberos_service_name)
    # Check if the user has a ticket in the credentials cache
    try:
      if call(['klist', '-s']) != 0:
        print_to_stderr(("-k requires a valid kerberos ticket but no valid kerberos "
                         "ticket found."))
        sys.exit(1)
    except OSError, e:
      print_to_stderr('klist not found on the system, install kerberos clients')
      sys.exit(1)
  elif options.use_ldap:
    print_to_stderr("Starting Impala Shell using LDAP-based authentication")
  else:
    print_to_stderr("Starting Impala Shell without Kerberos authentication")

  if options.ssl:
    if options.ca_cert is None:
      print_to_stderr("SSL is enabled. Impala server certificates will NOT be verified"\
                      " (set --ca_cert to change)")
    else:
      print_to_stderr("SSL is enabled")

  if options.output_file:
    try:
      # Make sure the given file can be opened for writing. This will also clear the file
      # if successful.
      open(options.output_file, 'wb')
    except IOError, e:
      print_to_stderr('Error opening output file for writing: %s' % e)
      sys.exit(1)

  if options.query or options.query_file:
    execute_queries_non_interactive_mode(options)
    sys.exit(0)

  intro = WELCOME_STRING
  shell = BlinkDBShell(options)
  while shell.is_alive:
    try:
      try:
        shell.cmdloop(intro)
      except KeyboardInterrupt:
        intro = '\n'
      # A last measure against any exceptions thrown by an rpc
      # not caught in the shell
      except socket.error, (code, e):
        # if the socket was interrupted, reconnect the connection with the client
        if code == errno.EINTR:
          print shell.CANCELLATION_MESSAGE
          shell._reconnect_cancellation()
        else:
          print_to_stderr("Socket error %s: %s" % (code, e))
          shell.imp_client.connected = False
          shell.prompt = shell.DISCONNECTED_PROMPT
      except DisconnectedException, e:
        # the client has lost the connection
        print_to_stderr(e)
        shell.imp_client.connected = False
        shell.prompt = shell.DISCONNECTED_PROMPT
      except QueryStateException, e:
        # an exception occurred while executing the query
        if shell._no_cancellation_error(e):
          shell.imp_client.close_query(shell.last_query_handle,
                                       shell.query_handle_closed)
          print_to_stderr(e)
      except RPCException, e:
        # could not complete the rpc successfully
        # suppress error if reason is cancellation
        if shell._no_cancellation_error(e):
          print_to_stderr(e)
    finally:
      intro = ''
