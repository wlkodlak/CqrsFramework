using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Text.RegularExpressions;

namespace CqrsFramework.InFile
{
    public class FileStreamProvider : IStreamProvider
    {
        private string _basePath;
        private Regex _regex;

        public FileStreamProvider(string basePath)
        {
            _regex = new Regex(@"^[a-zA-Z0-9._-]+", RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);
            _basePath = basePath ?? Environment.CurrentDirectory;
            Directory.CreateDirectory(_basePath);
        }

        public Stream Open(string name, FileMode fileMode)
        {
            if (!_regex.IsMatch(name))
                throw new ArgumentOutOfRangeException(string.Format("{0} is not allowed stream name", name));
            return File.Open(Path.Combine(_basePath, name), fileMode);
        }
    }
}
